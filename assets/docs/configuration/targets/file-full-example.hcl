# Extended configuration for File as a target (all options)

target {
  use "file" {
    # Path to output file
    path = "/path/to/output.txt"

    # File permissions (optional, default: 0644)
    permissions = "0644"

    # Whether to append to existing file (optional, default: false)
    append = true

    # Maximum file size in bytes before rotation (optional, default: 100MB)
    max_size = 104857600

    # Number of backup files to keep (optional, default: 3)
    max_backups = 3
  }
}