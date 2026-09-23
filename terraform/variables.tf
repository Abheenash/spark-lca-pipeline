variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name_prefix" {
  type    = string
  default = "lca"
}

variable "max_vcpu" {
  description = "Ceiling on the application's total capacity. EMR Serverless bills per vCPU-second, so this is the cost guardrail, not a performance setting."
  type        = number
  default     = 16

  validation {
    condition     = var.max_vcpu > 0 && var.max_vcpu <= 64
    error_message = "Keep the ceiling modest — this is a portfolio project, and EMR Serverless bills per vCPU-second."
  }
}
