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

AR_REPO="million-songs"
AR_IMAGE="pipeline"
IMAGE_TAG="$(date +%Y%m%d-%H%M%S)"
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${AR_IMAGE}:${IMAGE_TAG}"

CLOUD_RUN_INGEST_JOB="million-songs-ingest"
CLOUD_RUN_TRANSFORM_JOB="million-songs-transform"

echo "Setting project to $PROJECT_ID..."
gcloud config set project "$PROJECT_ID"

# --- Enable APIs ---
echo "Enabling APIs..."
gcloud services enable \
    storage.googleapis.com \
    bigquery.googleapis.com \
    run.googleapis.com \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com

# --- Service account ---
if gcloud iam service-accounts describe "$SA_EMAIL" &>/dev/null; then
    echo "Service account $SA_EMAIL already exists."
else
    echo "Creating service account..."
    gcloud iam service-accounts create "$SA_NAME" --display-name="Pipeline SA"
    echo "Waiting for service account to propagate..."
    sleep 10
fi

for role in \
    roles/storage.admin \
    roles/bigquery.admin \
    roles/run.admin \
    roles/artifactregistry.writer \
    roles/logging.logWriter; do
    echo "  Granting $role to $SA_EMAIL..."
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="$role" \
        --quiet > /dev/null
done

# Cloud Run Jobs run *as* our SA, so the SA must be allowed to act-as itself.
echo "  Granting roles/iam.serviceAccountUser on $SA_EMAIL (self-impersonation, needed for Cloud Run Jobs)..."
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/iam.serviceAccountUser" \
    --quiet > /dev/null

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
export CLOUD_RUN_INGEST_JOB=$CLOUD_RUN_INGEST_JOB
export CLOUD_RUN_TRANSFORM_JOB=$CLOUD_RUN_TRANSFORM_JOB
export PREFECT_API_URL=http://127.0.0.1:4200/api
EOF

# --- Terraform: bootstrap Artifact Registry first ---
# Cloud Run Jobs reference an image that must already exist in AR, so we
# apply the AR repo alone, push the image, then apply the full stack.
echo "Provisioning Artifact Registry (bootstrap)..."
cd terraform
terraform init
terraform apply -auto-approve \
    -target=google_artifact_registry_repository.pipeline \
    -var="project=$PROJECT_ID" \
    -var="gcs_bucket_name=${PROJECT_ID}-data" \
    -var="region=$REGION" \
    -var="location=$LOCATION" \
    -var="sa_email=$SA_EMAIL" \
    -var="image_uri=$IMAGE_URI"
cd ..

# --- Build the lean `jobs` image via Cloud Build and push to Artifact Registry ---
echo "Building jobs image via Cloud Build..."
echo "  Image: $IMAGE_URI"
gcloud builds submit \
    --service-account="projects/${PROJECT_ID}/serviceAccounts/${SA_EMAIL}" \
    --config=cloudbuild.yaml \
    --substitutions="_IMAGE=${IMAGE_URI}" \
    --region="$REGION" \
    .

# --- Terraform: full apply (data lake, DWH, Cloud Run Jobs) ---
echo "Provisioning infrastructure..."
cd terraform
terraform apply -auto-approve \
    -var="project=$PROJECT_ID" \
    -var="gcs_bucket_name=${PROJECT_ID}-data" \
    -var="region=$REGION" \
    -var="location=$LOCATION" \
    -var="sa_email=$SA_EMAIL" \
    -var="image_uri=$IMAGE_URI"
cd ..

echo ""
echo "=== Setup complete ==="
echo "Service account key: $KEY_PATH"
echo "Image: $IMAGE_URI"
echo "Run the pipeline with: docker compose up"
