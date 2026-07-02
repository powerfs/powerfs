savedcmd_powerfs.mod := printf '%s\n'   powerfs_mod.o powerfs_fs.o powerfs_transport.o | awk '!x[$$0]++ { print("./"$$0) }' > powerfs.mod
