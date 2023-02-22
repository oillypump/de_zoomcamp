# introduction

    1. what is Terraform?
        - an open-source tools by hashiCorp, used to provisioning infrastructure.
        - support DevOps best practices
        - Managing config files in source control to maintain an ideal provisioning state for testing and production env
    2. What is IaC?
        - Infrastructure as Code
    3. Some advantages
        - Infrastructure lifecycle management
        - version control commits
        - very useful for stack deployments with AWS, GCP, AZURE
        - State based approach

#### requirement:

    1. terraform client
    2. GCP account

# Initial Setup

1. Create an account with your Google email ID
2. Setup your first project if you haven't already
   - eg. "DTC DE Course", and note down the "Project ID" (we'll use this later when deploying infra with TF)
3. Setup service account & authentication for this project
4. Grant Viewer role to begin with.
   - Download service-account-keys (.json) for auth.
   - Download SDK for local setup
5. Set environment variable to point to your downloaded GCP keys:
   ```
   $ export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
   # Refresh token/session, and verify authentication
   $ gcloud auth application-default login
   ```

export GOOGLE_APPLICATION_CREDENTIALS=de-analytics-378504-17f8853d65d4.json
