variable "env" {
  description = "Deployment environment"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.env)
    error_message = "env must be one of dev, staging, prod."
  }
}

variable "project_name" {
  description = "Project name used in resource tags and names"
  type        = string
  default     = "laminar"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}
