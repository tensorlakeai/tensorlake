//! Region selection for Artifact Storage. Each region has independent repositories and credentials.

use crate::error::SdkError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactStorageRegion {
    UsEast1,
    EuCentral1,
}

impl ArtifactStorageRegion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UsEast1 => "us-east-1",
            Self::EuCentral1 => "eu-central-1",
        }
    }

    pub fn api_url(self) -> &'static str {
        match self {
            Self::UsEast1 => "https://api.tensorlake.ai",
            Self::EuCentral1 => "https://api.eu-central-1.tensorlake.ai",
        }
    }

    /// Select a production region without silently overriding development or custom endpoints.
    pub fn resolve_api_url(self, api_url: &str) -> Result<&'static str, SdkError> {
        if ![Self::UsEast1.api_url(), Self::EuCentral1.api_url()]
            .contains(&api_url.trim_end_matches('/'))
        {
            return Err(SdkError::ClientError(
                "Artifact Storage region cannot be combined with a custom or development API URL"
                    .to_string(),
            ));
        }
        Ok(self.api_url())
    }
}

impl std::str::FromStr for ArtifactStorageRegion {
    type Err = SdkError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "us-east-1" => Ok(Self::UsEast1),
            "eu-central-1" => Ok(Self::EuCentral1),
            _ => Err(SdkError::ClientError(format!(
                "Unsupported Artifact Storage region: {value}; expected us-east-1 or eu-central-1"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientBuilder, Sdk};

    #[test]
    fn regional_clients_route_data_and_mint_to_the_same_region() {
        let sdk =
            Sdk::with_client_builder(ClientBuilder::new("https://api.tensorlake.ai")).unwrap();
        for (region, api, git) in [
            (
                ArtifactStorageRegion::UsEast1,
                "https://api.tensorlake.ai",
                "https://git.tensorlake.ai",
            ),
            (
                ArtifactStorageRegion::EuCentral1,
                "https://api.eu-central-1.tensorlake.ai",
                "https://git.eu-central-1.tensorlake.ai",
            ),
        ] {
            let client = sdk.artifact_storage_in_region(region).unwrap();
            assert_eq!(client.git_base_url(), git);
            assert_eq!(client.api_client.as_ref().unwrap().base_url(), api);
            assert_eq!(
                region.as_str().parse::<ArtifactStorageRegion>().unwrap(),
                region
            );
        }
        assert_eq!(
            sdk.artifact_storage().unwrap().git_base_url(),
            "https://git.tensorlake.ai"
        );
    }

    #[test]
    fn invalid_regions_and_custom_endpoints_fail_closed() {
        for value in ["", "EU-CENTRAL-1", "eu-west-1", "../us-east-1"] {
            assert!(value.parse::<ArtifactStorageRegion>().is_err());
        }
        for api in [
            "https://api.tensorlake.dev",
            "http://localhost:3000",
            "https://api.tensorlake.ai.evil.test",
            "https://api.tensorlake.ai/path",
        ] {
            assert!(
                ArtifactStorageRegion::EuCentral1
                    .resolve_api_url(api)
                    .is_err()
            );
        }
    }
}
