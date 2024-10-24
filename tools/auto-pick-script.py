import os
import sys
from github import Github
import subprocess

# Get GitHub personal access token and other parameters
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
REPO_NAME = os.getenv('REPO_NAME', 'apache/doris')  # Default repository name
CONFLICT_LABEL = os.getenv('CONFLICT_LABEL', 'cherry-pick-conflict')  # Conflict label from environment variable

# Check if the required command-line arguments are provided
if len(sys.argv) != 3:
    print("Usage: python script.py <PR_NUMBER> <TARGET_BRANCH>")
    sys.exit(1)

pr_number = int(sys.argv[1])  # PR number from command-line argument
TARGET_BRANCH = sys.argv[2]    # Target branch from command-line argument

# Create GitHub instance
g = Github(GITHUB_TOKEN)
repo = g.get_repo(REPO_NAME)

# Get the specified PR
pr = repo.get_pull(pr_number)

# Check if the PR has been merged
if not pr.merged:
    print(f"PR #{pr_number} has not been merged yet.")
    exit(1)

merge_commit_sha = pr.merge_commit_sha

# Get the latest commit from the target branch
base_branch = repo.get_branch(TARGET_BRANCH)

# Create a new branch for cherry-picking the PR
new_branch_name = f'auto-pick-{pr.number}-{TARGET_BRANCH}'
repo.create_git_ref(ref=f'refs/heads/{new_branch_name}', sha=base_branch.commit.sha)
print(f"Created new branch {new_branch_name} from {TARGET_BRANCH}.")

# Clone the repository locally and switch to the new branch
repo_url = f"https://github.com/{REPO_NAME}.git"
subprocess.run(["git", "clone", repo_url])
repo_dir = REPO_NAME.split("/")[-1]  # Get the directory name
subprocess.run(["git", "checkout", new_branch_name], cwd=repo_dir)

# Set Git user identity for commits
subprocess.run(["git", "config", "user.email", "your-email@example.com"], cwd=repo_dir)
subprocess.run(["git", "config", "user.name", "Your Name"], cwd=repo_dir)


# Execute the cherry-pick operation
try:
    result = subprocess.run(["git", "cherry-pick", merge_commit_sha], cwd=repo_dir, check=True)
    print(f"Successfully cherry-picked commit {merge_commit_sha} into {new_branch_name}.")

    # Push the new branch
    subprocess.run(["git", "push", "origin", new_branch_name], cwd=repo_dir)

    # Create a new PR for the cherry-picked changes
    new_pr = repo.create_pull(
        title=f"Auto-pick PR #{pr.number} into {TARGET_BRANCH}",
        body=f"Cherry-pick of commits from PR #{pr.number} into {TARGET_BRANCH}.",
        head=new_branch_name,
        base=TARGET_BRANCH
    )
    print(f"Created a new PR #{new_pr.number} for cherry-picked changes.")
except subprocess.CalledProcessError:
    print(f"Conflict occurred while cherry-picking commit {merge_commit_sha}.")
    # Add conflict label
    pr.add_to_labels(CONFLICT_LABEL)
    print(f"Added label '{CONFLICT_LABEL}' to PR #{pr.number} due to conflict.")
