#!/usr/bin/env bash
# Tear down everything setup.sh provisioned for this project:
#   - terraform-managed resources (bucket, BQ dataset, AR repo, Cloud Run Jobs)
#   - the pipeline service account (gcloud-managed, not in terraform)
#   - local .keys/ and terraform state files
#
# Leaves the GCP project itself, enabled APIs, and the .env file intact so a
# re-run of ./setup.sh can immediately re-provision against the same project.
set -euo pipefail

if [[ ! -f .env ]]; then
    echo "error: .env not found — nothing to clean up (or setup.sh was never run)." >&2
    exit 1
fi

# shellcheck disable=SC1091
source .env

SA_EMAIL="million-songs-pipeline@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

echo "=== Million Songs Pipeline — Cleanup ==="
echo "Project: $GCP_PROJECT_ID"
echo ""

# --- Terraform-managed resources ---
echo "Destroying terraform-managed resources..."
cd terraform
terraform destroy -auto-approve \
    -var="project=$GCP_PROJECT_ID" \
    -var="gcs_bucket_name=$GCS_BUCKET" \
    -var="region=$REGION" \
    -var="location=$LOCATION" \
    -var="sa_email=dummy" \
    -var="image_uri=dummy"
cd ..

# --- Service account (gcloud-managed) ---
echo ""
if gcloud iam service-accounts describe "$SA_EMAIL" &>/dev/null; then
    echo "Deleting service account $SA_EMAIL..."
    gcloud iam service-accounts delete "$SA_EMAIL" --quiet
else
    echo "Service account $SA_EMAIL already gone, skipping."
fi

# --- Local artifacts ---
echo ""
echo "Removing local .keys/ and terraform state..."
rm -rf .keys
rm -f terraform/terraform.tfstate terraform/terraform.tfstate.backup

echo ""
echo "=== Cleanup complete ==="
echo "Project $GCP_PROJECT_ID is empty. Re-run ./setup.sh to reprovision."
