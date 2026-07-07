use bytes::Bytes;
use chrono::Utc;
use powerfs_common::{
    constants::{NEEDLE_CHECKSUM_SIZE, NEEDLE_FOOTER_SIZE, NEEDLE_HEADER_SIZE, NEEDLE_ID_SIZE},
    error::{PowerFsError, Result},
    types::{ChecksumAlgorithm, NeedleId, NeedleInfo, VolumeId},
    utils::Checksum,
};
use std::io::{Read, Seek, SeekFrom, Write};

#[derive(Debug, Clone)]
pub struct Needle {
    pub id: NeedleId,
    pub volume_id: VolumeId,
    pub data: Bytes,
    pub offset: u64,
    pub checksum: u64,
    pub checksum_algorithm: ChecksumAlgorithm,
}

#[allow(clippy::result_large_err)]
impl Needle {
    pub fn new(id: NeedleId, volume_id: VolumeId, data: Bytes) -> Self {
        Self::new_with_algorithm(id, volume_id, data, ChecksumAlgorithm::default())
    }

    pub fn new_with_algorithm(
        id: NeedleId,
        volume_id: VolumeId,
        data: Bytes,
        algorithm: ChecksumAlgorithm,
    ) -> Self {
        let checksum_val = Checksum::compute(&data, algorithm);
        Needle {
            id,
            volume_id,
            data,
            offset: 0,
            checksum: checksum_val.as_u64(),
            checksum_algorithm: algorithm,
        }
    }

    pub fn size(&self) -> usize {
        NEEDLE_HEADER_SIZE + self.data.len() + NEEDLE_FOOTER_SIZE
    }

    pub fn data_size(&self) -> usize {
        self.data.len()
    }

    pub fn to_bytes(&self) -> Bytes {
        let total_size = self.size();
        let mut buf = Vec::with_capacity(total_size);

        buf.extend_from_slice(&self.id.0.to_be_bytes());

        let data_size = self.data.len() as u32;
        buf.extend_from_slice(&data_size.to_be_bytes());

        buf.extend_from_slice(&self.data);

        buf.extend_from_slice(&self.checksum.to_be_bytes());

        Bytes::from(buf)
    }

    pub fn from_bytes(bytes: &[u8], volume_id: VolumeId, offset: u64) -> Result<Self> {
        if bytes.len() < NEEDLE_HEADER_SIZE + NEEDLE_FOOTER_SIZE {
            return Err(PowerFsError::InvalidRequest(
                "needle data too short".to_string(),
            ));
        }

        let mut id_bytes = [0u8; NEEDLE_ID_SIZE];
        id_bytes.copy_from_slice(&bytes[0..NEEDLE_ID_SIZE]);
        let id = NeedleId(u64::from_be_bytes(id_bytes));

        let mut data_size_bytes = [0u8; 4];
        data_size_bytes.copy_from_slice(&bytes[NEEDLE_ID_SIZE..NEEDLE_ID_SIZE + 4]);
        let data_size = u32::from_be_bytes(data_size_bytes) as usize;

        let expected_total_size = NEEDLE_HEADER_SIZE + data_size + NEEDLE_FOOTER_SIZE;
        if bytes.len() != expected_total_size {
            return Err(PowerFsError::InvalidRequest(
                "needle data size mismatch".to_string(),
            ));
        }

        let data_start = NEEDLE_HEADER_SIZE;
        let data_end = data_start + data_size;
        let data = Bytes::copy_from_slice(&bytes[data_start..data_end]);

        let checksum_start = data_end;
        let mut checksum_bytes = [0u8; NEEDLE_CHECKSUM_SIZE];
        checksum_bytes
            .copy_from_slice(&bytes[checksum_start..checksum_start + NEEDLE_CHECKSUM_SIZE]);
        let checksum = u64::from_be_bytes(checksum_bytes);

        let calculated_checksum = Checksum::compute(&data, ChecksumAlgorithm::default());
        if checksum != calculated_checksum.as_u64() {
            return Err(PowerFsError::ChecksumMismatch);
        }

        Ok(Needle {
            id,
            volume_id,
            data,
            offset,
            checksum,
            checksum_algorithm: ChecksumAlgorithm::default(),
        })
    }

    pub fn read_from<R: Read + Seek>(
        reader: &mut R,
        offset: u64,
        volume_id: VolumeId,
    ) -> Result<Self> {
        reader.seek(SeekFrom::Start(offset))?;

        let mut id_bytes = [0u8; NEEDLE_ID_SIZE];
        reader.read_exact(&mut id_bytes)?;
        let id = NeedleId(u64::from_be_bytes(id_bytes));

        let mut data_size_bytes = [0u8; 4];
        reader.read_exact(&mut data_size_bytes)?;
        let data_size = u32::from_be_bytes(data_size_bytes) as usize;

        let mut data = vec![0u8; data_size];
        reader.read_exact(&mut data)?;

        let mut checksum_bytes = [0u8; NEEDLE_CHECKSUM_SIZE];
        reader.read_exact(&mut checksum_bytes)?;
        let checksum = u64::from_be_bytes(checksum_bytes);

        let calculated_checksum = Checksum::compute(&data, ChecksumAlgorithm::default());
        if checksum != calculated_checksum.as_u64() {
            return Err(PowerFsError::ChecksumMismatch);
        }

        Ok(Needle {
            id,
            volume_id,
            data: Bytes::from(data),
            offset,
            checksum,
            checksum_algorithm: ChecksumAlgorithm::default(),
        })
    }

    pub fn write_to<W: Write + Seek>(&self, writer: &mut W, offset: u64) -> Result<()> {
        writer.seek(SeekFrom::Start(offset))?;

        writer.write_all(&self.id.0.to_be_bytes())?;

        let data_size = self.data.len() as u32;
        writer.write_all(&data_size.to_be_bytes())?;

        writer.write_all(&self.data)?;

        writer.write_all(&self.checksum.to_be_bytes())?;

        Ok(())
    }

    pub fn to_info(&self) -> NeedleInfo {
        NeedleInfo {
            id: self.id.clone(),
            volume_id: self.volume_id,
            data_size: self.data.len() as u32,
            offset: self.offset,
            checksum: self.checksum,
            checksum_algorithm: self.checksum_algorithm,
            last_verified_at: None,
            verification_count: 0,
            deleted_at: None,
            delete_retention_until: None,
            worm_retention_until: None,
            created_at: Utc::now(),
            ec_enabled: false,
            ec_k: None,
            ec_m: None,
            ec_shards: Vec::new(),
        }
    }
}
