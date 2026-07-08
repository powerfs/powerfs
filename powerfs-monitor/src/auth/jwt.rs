use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String, // user_id
    pub username: String,
    pub role: String, // admin / user
    pub exp: usize,   // expiration timestamp
    pub iat: usize,   // issued at
}

#[derive(Debug, Clone)]
pub struct TokenPair {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: u64,
}

pub struct JwtValidator {
    decoding_key: DecodingKey,
    encoding_key: EncodingKey,
    refresh_decoding_key: DecodingKey,
    refresh_encoding_key: EncodingKey,
    access_ttl: Duration,
    refresh_ttl: Duration,
}

impl JwtValidator {
    pub fn new(secret: &str) -> Self {
        Self {
            decoding_key: DecodingKey::from_secret(secret.as_bytes()),
            encoding_key: EncodingKey::from_secret(secret.as_bytes()),
            refresh_decoding_key: DecodingKey::from_secret(
                format!("{}_refresh", secret).as_bytes(),
            ),
            refresh_encoding_key: EncodingKey::from_secret(
                format!("{}_refresh", secret).as_bytes(),
            ),
            access_ttl: Duration::minutes(15),
            refresh_ttl: Duration::days(7),
        }
    }

    pub fn generate_token_pair(&self, user_id: &str, username: &str, role: &str) -> TokenPair {
        let now = Utc::now();
        let access_exp = now + self.access_ttl;
        let refresh_exp = now + self.refresh_ttl;

        let access_claims = Claims {
            sub: user_id.to_string(),
            username: username.to_string(),
            role: role.to_string(),
            exp: access_exp.timestamp() as usize,
            iat: now.timestamp() as usize,
        };

        let refresh_claims = Claims {
            sub: user_id.to_string(),
            username: username.to_string(),
            role: role.to_string(),
            exp: refresh_exp.timestamp() as usize,
            iat: now.timestamp() as usize,
        };

        let access_token = encode(&Header::default(), &access_claims, &self.encoding_key)
            .expect("Failed to encode access token");
        let refresh_token = encode(
            &Header::default(),
            &refresh_claims,
            &self.refresh_encoding_key,
        )
        .expect("Failed to encode refresh token");

        TokenPair {
            access_token,
            refresh_token,
            expires_in: self.access_ttl.num_seconds() as u64,
        }
    }

    pub fn validate_access_token(&self, token: &str) -> Result<Claims, String> {
        let token = token.trim_start_matches("Bearer ").trim();
        decode::<Claims>(token, &self.decoding_key, &Validation::default())
            .map(|data| data.claims)
            .map_err(|e| format!("Invalid access token: {}", e))
    }

    pub fn validate_refresh_token(&self, token: &str) -> Result<Claims, String> {
        let token = token.trim_start_matches("Bearer ").trim();
        decode::<Claims>(token, &self.refresh_decoding_key, &Validation::default())
            .map(|data| data.claims)
            .map_err(|e| format!("Invalid refresh token: {}", e))
    }

    pub fn refresh_access_token(&self, refresh_token: &str) -> Result<TokenPair, String> {
        let claims = self.validate_refresh_token(refresh_token)?;
        Ok(self.generate_token_pair(&claims.sub, &claims.username, &claims.role))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_generation_and_validation() {
        let validator = JwtValidator::new("test_secret");
        let tokens = validator.generate_token_pair("user1", "alice", "user");

        let claims = validator.validate_access_token(&tokens.access_token);
        assert!(claims.is_ok());
        assert_eq!(claims.unwrap().username, "alice");

        let refresh_result = validator.refresh_access_token(&tokens.refresh_token);
        assert!(refresh_result.is_ok());
    }
}
