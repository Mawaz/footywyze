import subprocess
import datetime

def get_previous_commit_hash():
    # Get the hash of the previous commit
    return subprocess.check_output(["git", "rev-parse", "HEAD~1"]).strip().decode()

def get_git_diff(last_commit_hash):
    # Get the diff from the last commit to the current state
    diff = subprocess.check_output(["git", "diff", last_commit_hash]).decode()
    return diff

def format_diff_for_changelog(diff):
    # Format the diff for readability in the changelog
    formatted_diff = "\n```\n" + diff + "\n```\n"
    return formatted_diff

def prepend_to_changelog(formatted_diff):
    changelog_path = "CHANGELOG.md"
    with open(changelog_path, 'r') as file:
        current_content = file.read()
    
    # Format the new content for the changelog
    new_content = f"## Detailed Changes - {datetime.datetime.now().strftime('%Y-%m-%d')}\n{formatted_diff}\n"
    
    # Prepend the new content to the current content
    with open(changelog_path, 'w') as file:
        file.write(new_content + current_content)

if __name__ == "__main__":
    last_commit_hash = get_previous_commit_hash()
    diff = get_git_diff(last_commit_hash)
    if diff:
        formatted_diff = format_diff_for_changelog(diff)
        prepend_to_changelog(formatted_diff)
    else:
        print("No new changes since the last commit.")