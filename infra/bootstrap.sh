#!/usr/bin/env bash
# One-time AWS bootstrap for tv-audience-metrics-pipeline.
#
# Creates (re-runnable: checks existence before creating, never fails on re-run):
#   1. GitHub OIDC identity provider
#   2. IAM Role assumable only via OIDC, trust policy restricted to this repo
#   3. Private S3 bucket (Block Public Access, SSE-S3, HTTPS-only policy)
#
# This script is NOT part of the pipeline itself (constitution Principle II) — it is run
# manually, once, from a local machine with AWS admin credentials. It never runs in
# GitHub Actions and never handles long-lived pipeline credentials.
#
# Usage:
#   AWS_REGION=us-east-1 \
#   BUCKET_NAME=tv-audience-metrics-pipeline-poc \
#   GITHUB_ORG=<your-github-org-or-user> \
#   GITHUB_REPO=tv-audience-metrics-pipeline \
#   ./infra/bootstrap.sh

set -euo pipefail

AWS_REGION="${AWS_REGION:?Set AWS_REGION, e.g. us-east-1}"
BUCKET_NAME="${BUCKET_NAME:?Set BUCKET_NAME to a globally-unique S3 bucket name}"
GITHUB_ORG="${GITHUB_ORG:?Set GITHUB_ORG to your GitHub user/org}"
GITHUB_REPO="${GITHUB_REPO:-tv-audience-metrics-pipeline}"
ROLE_NAME="${ROLE_NAME:-tv-audience-metrics-pipeline-oidc-role}"
GITHUB_OIDC_URL="https://token.actions.githubusercontent.com"
# Fixed thumbprint published by GitHub for its OIDC provider (root CA used by all repos).
GITHUB_OIDC_THUMBPRINT="6938fd4d98bab03faadb97b34396831e3780aea"

echo "== 1/3: GitHub OIDC identity provider =="
OIDC_ARN=$(aws iam list-open-id-connect-providers \
    --query "OpenIDConnectProviderList[?contains(Arn, 'token.actions.githubusercontent.com')].Arn" \
    --output text)

if [[ -z "${OIDC_ARN}" ]]; then
    OIDC_ARN=$(aws iam create-open-id-connect-provider \
        --url "${GITHUB_OIDC_URL}" \
        --client-id-list "sts.amazonaws.com" \
        --thumbprint-list "${GITHUB_OIDC_THUMBPRINT}" \
        --query "OpenIDConnectProviderArn" --output text)
    echo "Created OIDC provider: ${OIDC_ARN}"
else
    echo "OIDC provider already exists: ${OIDC_ARN}"
fi

echo "== 2/3: IAM Role restricted to ${GITHUB_ORG}/${GITHUB_REPO} =="
TRUST_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Federated": "${OIDC_ARN}" },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": { "token.actions.githubusercontent.com:aud": "sts.amazonaws.com" },
        "StringLike": { "token.actions.githubusercontent.com:sub": "repo:${GITHUB_ORG}/${GITHUB_REPO}:*" }
      }
    }
  ]
}
EOF
)

if aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
    echo "Role ${ROLE_NAME} already exists, updating trust policy"
    aws iam update-assume-role-policy --role-name "${ROLE_NAME}" --policy-document "${TRUST_POLICY}"
else
    aws iam create-role \
        --role-name "${ROLE_NAME}" \
        --assume-role-policy-document "${TRUST_POLICY}" \
        --description "OIDC role for tv-audience-metrics-pipeline GitHub Actions" >/dev/null
    echo "Created role: ${ROLE_NAME}"
fi

ROLE_ARN=$(aws iam get-role --role-name "${ROLE_NAME}" --query "Role.Arn" --output text)

# Least privilege: only the 3 actions the pipeline needs, scoped to this bucket's ARN.
# ListBucket is required (not GetObject) to discover which objects exist under a
# partition prefix before the delete-then-write step — scoped to the bucket resource
# itself, never "Resource": "*".
S3_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBucketForDeleteThenWrite",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${BUCKET_NAME}"
    },
    {
      "Sid": "ReadWriteDeleteObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::${BUCKET_NAME}/*"
    }
  ]
}
EOF
)

aws iam put-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-name "tv-audience-metrics-pipeline-s3-access" \
    --policy-document "${S3_POLICY}"
echo "Attached least-privilege S3 policy to ${ROLE_NAME}"

echo "== 3/3: Private, encrypted S3 bucket =="
if aws s3api head-bucket --bucket "${BUCKET_NAME}" 2>/dev/null; then
    echo "Bucket ${BUCKET_NAME} already exists"
else
    if [[ "${AWS_REGION}" == "us-east-1" ]]; then
        aws s3api create-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}" >/dev/null
    else
        aws s3api create-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}" \
            --create-bucket-configuration "LocationConstraint=${AWS_REGION}" >/dev/null
    fi
    echo "Created bucket: ${BUCKET_NAME}"
fi

aws s3api put-public-access-block \
    --bucket "${BUCKET_NAME}" \
    --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-encryption \
    --bucket "${BUCKET_NAME}" \
    --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

DENY_INSECURE_TRANSPORT_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyInsecureTransport",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": ["arn:aws:s3:::${BUCKET_NAME}", "arn:aws:s3:::${BUCKET_NAME}/*"],
      "Condition": { "Bool": { "aws:SecureTransport": "false" } }
    }
  ]
}
EOF
)
aws s3api put-bucket-policy --bucket "${BUCKET_NAME}" --policy "${DENY_INSECURE_TRANSPORT_POLICY}"

echo ""
echo "Bootstrap complete. Configure these as GitHub repo Secrets/Variables:"
echo "  ROLE_ARN=${ROLE_ARN}"
echo "  BUCKET_NAME=${BUCKET_NAME}"
echo "  AWS_REGION=${AWS_REGION}"
