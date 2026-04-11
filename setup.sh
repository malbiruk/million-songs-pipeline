#!/usr/bin/env bash
set -euo pipefail

echo "=== Million Songs Pipeline — Setup ==="

# --- GCP project ---
read -rp "GCP Project ID: " PROJECT_ID
read -rp "GCP Region [europe-central2]: " REGION
REGION="${REGION:-europe-central2}"
# Derive multi-region location from region
if [[ "$REGION" == europe-* ]]; then
    LOCATION="EU"
elif [[ "$REGION" == us-* ]]; then
    LOCATION="US"
else
    LOCATION="US"
fi
SA_NAME="million-songs-pipeline"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
KEY_PATH="$(pwd)/.keys/sa-key.json"

echo "Setting project to $PROJECT_ID..."
gcloud config set project "$PROJECT_ID"

# --- Enable APIs ---
echo "Enabling APIs..."
gcloud services enable storage.googleapis.com bigquery.googleapis.com

# --- Service account ---
if gcloud iam service-accounts describe "$SA_EMAIL" &>/dev/null; then
    echo "Service account $SA_EMAIL already exists."
else
    echo "Creating service account..."
    gcloud iam service-accounts create "$SA_NAME" --display-name="Pipeline SA"
    echo "Waiting for service account to propagate..."
    sleep 10
fi



echo "Assigning roles..."
for role in roles/storage.admin roles/bigquery.admin; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="$role" \
        --quiet > /dev/null
done

echo "Generating key..."
mkdir -p "$(dirname "$KEY_PATH")"
gcloud iam service-accounts keys create "$KEY_PATH" \
    --iam-account="$SA_EMAIL"

# --- .env ---
echo "Writing .env..."
cat > .env <<EOF
export GOOGLE_APPLICATION_CREDENTIALS=$KEY_PATH
export GCP_PROJECT_ID=$PROJECT_ID
export GCS_BUCKET=${PROJECT_ID}-data
export BQ_DATASET=million_songs
export REGION=$REGION
export LOCATION=$LOCATION
export PREFECT_API_URL=http://127.0.0.1:4200/api
EOF

# --- Terraform ---
echo "Provisioning infrastructure..."
cd terraform
terraform init
terraform apply -auto-approve -var="project=$PROJECT_ID" -var="gcs_bucket_name=${PROJECT_ID}-data" -var="region=$REGION" -var="location=$LOCATION"
cd ..

echo ""
echo "=== Setup complete ==="
echo "Service account key: $KEY_PATH"
echo "Run the pipeline with: docker compose up"
