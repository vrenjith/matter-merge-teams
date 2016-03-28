# Merge Mattermost Teams
This is a Java console application that can help migrate an existing Mattermost installation with multiple teams into an instance with a single team. This takes care of the following:

1. Merge all the existing teams into one
1. Migrate all users from the existing teams to one team
1. Migrate all posts from the existing teams
1. Migrate all channels to the destination channel
1. Migrate all user preferences
1. Merge the personal chats from multiple teams into the destination single team
1. Retain all incoming webhooks data
1. Cleanup invalid accounts
1. Cleanup userids to match email accounts

Warning

1. This deletes all session data

Read the code before using this.
