use powerfs_master::directory_tree::DirectoryTree;
use tempfile::TempDir;

fn setup_tree() -> (DirectoryTree, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let tree = DirectoryTree::new(temp_dir.path()).unwrap();
    tree.init_root().unwrap();
    (tree, temp_dir)
}

#[test]
fn test_register_job_client_first_client() {
    let (tree, _td) = setup_tree();

    let result = tree.register_job_client("job-001", "training-job", "client-a");
    assert!(result);

    let info = tree.get_job_info("job-001").unwrap();
    assert_eq!(info.job_id, "job-001");
    assert_eq!(info.job_name, "training-job");
    assert_eq!(info.client_ids.len(), 1);
    assert!(info.client_ids.contains("client-a"));
    assert!(info.is_active);
    assert!(info.start_time > 0);
    assert_eq!(info.end_time, 0);
}

#[test]
fn test_register_job_client_multiple_clients() {
    let (tree, _td) = setup_tree();

    tree.register_job_client("job-001", "training-job", "client-a");
    tree.register_job_client("job-001", "training-job", "client-b");
    tree.register_job_client("job-001", "training-job", "client-c");

    let info = tree.get_job_info("job-001").unwrap();
    assert_eq!(info.client_ids.len(), 3);
    assert!(info.client_ids.contains("client-a"));
    assert!(info.client_ids.contains("client-b"));
    assert!(info.client_ids.contains("client-c"));
}

#[test]
fn test_register_job_client_duplicate() {
    let (tree, _td) = setup_tree();

    tree.register_job_client("job-001", "training", "client-a");
    tree.register_job_client("job-001", "training", "client-a");

    let info = tree.get_job_info("job-001").unwrap();
    assert_eq!(info.client_ids.len(), 1);
}

#[test]
fn test_deregister_job_client() {
    let (tree, _td) = setup_tree();

    tree.register_job_client("job-001", "job", "client-a");
    tree.register_job_client("job-001", "job", "client-b");

    let result = tree.deregister_job_client("job-001", "client-a");
    assert!(result);

    let info = tree.get_job_info("job-001").unwrap();
    assert_eq!(info.client_ids.len(), 1);
    assert!(info.client_ids.contains("client-b"));
    assert!(info.is_active);
}

#[test]
fn test_deregister_last_client_deactivates_job() {
    let (tree, _td) = setup_tree();

    tree.register_job_client("job-001", "job", "client-a");

    let result = tree.deregister_job_client("job-001", "client-a");
    assert!(result);

    let info = tree.get_job_info("job-001").unwrap();
    assert!(!info.is_active);
    assert!(info.end_time > 0);
    assert_eq!(info.client_ids.len(), 0);
}

#[test]
fn test_deregister_nonexistent_job() {
    let (tree, _td) = setup_tree();

    let result = tree.deregister_job_client("nonexistent", "client-a");
    assert!(!result);
}

#[test]
fn test_complete_job() {
    let (tree, _td) = setup_tree();

    tree.register_job_client("job-001", "job", "client-a");
    tree.register_job_client("job-001", "job", "client-b");

    let result = tree.complete_job("job-001");
    assert_eq!(result, Some(2));

    let info = tree.get_job_info("job-001").unwrap();
    assert!(!info.is_active);
    assert!(info.end_time > 0);
}

#[test]
fn test_complete_nonexistent_job() {
    let (tree, _td) = setup_tree();

    let result = tree.complete_job("nonexistent");
    assert_eq!(result, None);
}

#[test]
fn test_get_job_info_nonexistent() {
    let (tree, _td) = setup_tree();

    let info = tree.get_job_info("nonexistent");
    assert!(info.is_none());
}

#[test]
fn test_is_job_active() {
    let (tree, _td) = setup_tree();

    assert!(!tree.is_job_active("job-001"));

    tree.register_job_client("job-001", "job", "client-a");
    assert!(tree.is_job_active("job-001"));

    tree.complete_job("job-001");
    assert!(!tree.is_job_active("job-001"));
}

#[test]
fn test_multiple_jobs_independent() {
    let (tree, _td) = setup_tree();

    tree.register_job_client("job-a", "Job A", "client-1");
    tree.register_job_client("job-b", "Job B", "client-2");

    assert!(tree.is_job_active("job-a"));
    assert!(tree.is_job_active("job-b"));

    tree.complete_job("job-a");

    assert!(!tree.is_job_active("job-a"));
    assert!(tree.is_job_active("job-b"));

    let info_b = tree.get_job_info("job-b").unwrap();
    assert_eq!(info_b.job_name, "Job B");
}

#[test]
fn test_job_registration_name_uses_first_registration() {
    let (tree, _td) = setup_tree();

    tree.register_job_client("job-001", "First Name", "client-a");
    tree.register_job_client("job-001", "Second Name", "client-b");

    let info = tree.get_job_info("job-001").unwrap();
    assert_eq!(info.job_name, "First Name");
}
