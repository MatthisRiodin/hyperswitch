#!/bin/bash
set -e

echo "Deploying Hyperswitch features..."

# Superposition configurations
SUPERPOSITION_URL="http://localhost:8081"
WORKSPACE_ID="dev"
ORG_ID="localorg"

# Function to update superposition config
update_superposition_config() {
  local key=$1
  local value=$2
  local description=$3
  echo "Setting Superposition config: $key = $value"
  
  curl -X PUT -sS -o /dev/null -w "HTTP Status: %{http_code}\n" -H "Content-Type: application/json" -H "x-org-id: $ORG_ID" -H "x-tenant: $ORG_ID" -H "x-workspace: $WORKSPACE_ID" -d '{
    "key": "'"$key"'",
    "value": '"$value"',
    "schema": {"type": "boolean"},
    "description": "'"$description"'",
    "change_reason": "Terraform deployment"
  }' "$SUPERPOSITION_URL/default-config/$key"
}

# Cost Observability & Revenue Recovery (GSM)
update_superposition_config "should_call_gsm" "true" "Whether to call GSM for auto-retries on payment failure"

# Payments Modules
update_superposition_config "should_call_pm_modular_service" "true" "Whether to call PM Modular service"

# Additional Features (Eligibility, Level 2/3 Data, MIT)
update_superposition_config "should_perform_eligibility" "true" "Whether the SDK should perform an eligibility check before confirming payment"
update_superposition_config "should_enable_mit_with_limited_card_data" "true" "Whether to allow merchant-initiated transactions (MIT) with limited card data"
update_superposition_config "enable_extended_card_bin" "true" "Whether to use extended card BIN (8-digit) for payment method data"
update_superposition_config "gsm_payout_call" "true" "Whether to call GSM for payout retries"

# Update Postgres Database for Merchant Account and Business Profile
echo "Updating PostgreSQL Database..."
docker compose exec -T pg psql -U db_user -d hyperswitch_db -c "
  UPDATE merchant_account SET 
    is_recon_enabled = true,
    recon_status = 'active';

  UPDATE business_profile SET 
    max_auto_retries_enabled = true,
    is_click_to_pay_enabled = true,
    is_network_tokenization_enabled = true;
"

echo "Deployment complete!"
