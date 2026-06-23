terraform {
  required_version = ">= 1.0.0"
}

resource "null_resource" "deploy_hyperswitch_features" {
  # This resource will execute the setup_features.sh script locally
  # whenever we want to deploy the latest feature configs.
  
  provisioner "local-exec" {
    command = "bash ${path.module}/setup_features.sh"
  }

  # Add triggers if you want the script to run every time
  triggers = {
    always_run = "${timestamp()}"
  }
}
