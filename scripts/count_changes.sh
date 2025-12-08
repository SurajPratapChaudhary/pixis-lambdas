#!/bin/bash

commit_sha=$1
shift  # Shift the arguments to access folders as $@

# This script counts changed files in specified folders
echo "::group::Counting Changes"
if [ "$(git rev-list --count $commit_sha)" -eq "1" ]; then
    # It's the first commit, count all files in specified folder
    for folder in "$@"
    do
        # Normalize the folder name for output variables by replacing dashes with underscores
        output_folder_name=$(echo "$folder" | tr '/' '_' | sed 's/-/_/g')
        # Count files, ensuring to match the exact folder path
        file_count=$(git ls-tree -r $commit_sha --name-only | grep -c "^$folder")
        echo "${output_folder_name}_count=$file_count" >> $GITHUB_OUTPUT
    done
else
    # Not the first commit, perform diff
    for folder in "$@"
    do
        output_folder_name=$(echo "$folder" | tr '/' '_' | sed 's/-/_/g')
        file_count=$(git diff-tree --no-commit-id --name-only -r $commit_sha | grep -c "^$folder")
        echo "${output_folder_name}_count=$file_count" >> $GITHUB_OUTPUT
    done
fi
echo "::endgroup::"
