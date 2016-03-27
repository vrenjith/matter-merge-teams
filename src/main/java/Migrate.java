import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Renjith Pillai on 18/03/16.
 */


public class Migrate
{
    public static void main(String ...args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, ParseException
    {
        Options options = new Options();
        options.addOption("t", true, "target team handle");
        options.addOption("h", true, "postgresql database host");
        options.addOption("u", true, "database username");
        options.addOption("p", true, "database password");
        options.addOption("d", true, "database name");
        options.addOption("r", true, "database port");
        options.addOption("a", true, "admin email address");
        options.addOption("m", true, "allowed email domains (pipe seperated)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse( options, args);
        Class.forName("org.postgresql.Driver").newInstance();

        Connection conn = null;

        try {
            conn = DriverManager.getConnection(String.format("jdbc:postgresql://%s:%s/%s",
                    cmd.getOptionValue("h"), cmd.getOptionValue("r"), cmd.getOptionValue("d")),
                    cmd.getOptionValue("u"), cmd.getOptionValue("p"));

            //Update all usernames
            exst(conn, String.format("DELETE FROM CHANNELMEMBERS WHERE userid IN (SELECT id FROM USERS WHERE lower(email) NOT SIMILAR TO '%(%s)')", cmd.getOptionValue("m")));
            exst(conn, String.format("DELETE FROM POSTS WHERE userid IN (SELECT id FROM USERS WHERE lower(email) NOT SIMILAR TO '%(%s)')", cmd.getOptionValue("m")));
            exst(conn, String.format("DELETE FROM USERS WHERE lower(email) NOT SIMILAR TO '%(%s)'", cmd.getOptionValue("m")));

            //Update all userids to email address left part
            exst(conn, "UPDATE USERS SET username = lower(substring(email from '#\"%#\"@%' for '#'));");

            //User updates
            final List<User> allUsers = getAllUsers(conn);
            final List<String> distinctEmailIDs = getDistinctEmailIDs(conn);
            final String targetTeamId = getTargetTeamId(conn, cmd.getOptionValue("t"));
            final Map<String, User> targetExistingMembers = getExistingMembers(conn, targetTeamId);

            final Map<String, String> oldVsNewUser = new HashMap<String, String>();

            for (String email :
                    distinctEmailIDs)
            {
                if (! targetExistingMembers.containsKey(email)) //Not Part of the target team
                {
                    User pickedUpUser = null;
                    for (User user : allUsers)
                    {
                        if(user.getEmail().toLowerCase().equals(email.toLowerCase()))
                        {
                            if(null == pickedUpUser)
                            {
                                //This means, we are just finding the user to be migrated into target team
                                pickedUpUser = user;
                                exst(conn, String.format("UPDATE USERS SET teamid = '%s' WHERE id = '%s'", targetTeamId, user.getId()));
                                pickedUpUser.setTeamId(targetTeamId);//Just for having correct data
                            }
                            else
                            {
                                //We already picked up one. Swap all the posts to this user
                                exst(conn, String.format("UPDATE POSTS SET userid = '%s' WHERE userid = '%s'", pickedUpUser.getId(), user.getId()));
                            }
                            oldVsNewUser.put(user.getId(), pickedUpUser.getId());
                        }
                    }
                }
                else
                {
                    User pickedUpUser = targetExistingMembers.get(email);
                    for (User user : allUsers)
                    {
                        if(user.getEmail().equals(email))
                        {
                            oldVsNewUser.put(user.getId(), pickedUpUser.getId());
                            if(! pickedUpUser.getId().equals(user.getId()))
                            {
                                exst(conn, String.format("UPDATE POSTS SET userid = '%s' WHERE userid = '%s'", pickedUpUser.getId(), user.getId()));
                            }
                            else
                            {
                                //System.out.println("We know this");
                            }
                        }
                    }
                }
            }

            //Update non target public channels to target team id
            final List<Channel> publicChannels = getNonTargetPublicChannels(conn, targetTeamId);
            for(Channel channel : publicChannels)
            {
                if (channel.getDisplayName().equals("town-square") || channel.getDisplayName().equals("off-topic"))
                {
                    exst(conn, String.format("UPDATE CHANNELS SET displayname = '%s-%s', name = '%s-%s', teamid = '%s' " +
                            " WHERE id = '%s'",
                            channel.getTeamDisplayName().replaceAll("'", "''"),
                            channel.getDisplayName().replaceAll("'", "''"),
                            channel.getTeamName(),
                            channel.getName(),
                            targetTeamId,
                            channel.getId()));
                }
                else
                {
                    exst(conn, String.format("UPDATE CHANNELS SET displayname = '%s-%s', name = '%s-%s', teamid = '%s' " +
                        ", creatorid = '%s' WHERE id = '%s'",
                            channel.getTeamDisplayName().replaceAll("'", "''"),
                            channel.getDisplayName().replaceAll("'", "''"),
                            channel.getTeamName(),
                            channel.getName(),
                            targetTeamId,
                            oldVsNewUser.get(channel.getCreatorId()),
                            channel.getId()));
                }
            }

            final List<Channel> nonTargetDirectChannels = getNonTargetDirectChannels(conn, targetTeamId);
            for (Channel channel : nonTargetDirectChannels)
            {
                if(!areThereMatchingUserIds(channel.getName(), oldVsNewUser))
                {
                    continue;
                }
                System.out.println(String.format("[%s] [%s] [%s] [%s] [%s]",
                        channel.getId(), channel.getName(), channel.getTeamId(), channel.getTeamName(), channel.getCreatorId()));
                final String newChannelName = getNewChannelName(channel.getName(), oldVsNewUser);

                final String existingChannel = getExistingChannel(conn, newChannelName, targetTeamId);
                if(null == existingChannel)
                {
                    exst(conn, String.format("UPDATE CHANNELS SET name = '%s', teamid = '%s' " +
                                    "WHERE id = '%s'",
                            newChannelName,
                            targetTeamId,
                            channel.getId())
                    );
                }
                else
                {
                    //From-To
                    exst(conn, String.format("UPDATE POSTS SET channelid = '%s' WHERE channelid = '%s'",
                            existingChannel,
                            channel.getId()));
                }
            }

            //Private channels
            final List<Channel> nonTargetPrivateChannels = getNonTargetPrivateChannels(conn, targetTeamId);
            for (Channel channel : nonTargetPrivateChannels)
            {
                exst(conn, String.format("UPDATE CHANNELS SET teamid = '%s' " +
                                ", creatorid = '%s' WHERE id = '%s'",
                        targetTeamId,
                        oldVsNewUser.get(channel.getCreatorId()),
                        channel.getId()));
            }

            //Update userid in audits - Moved to delete

            //Update userid, teamid in incomingwebhooks
            Map<String,String> incomingWebHooks = getIncomingWebHooks(conn);
            for (Map.Entry<String, String> entry : incomingWebHooks.entrySet())
            {
                exst(conn, String.format("UPDATE INCOMINGWEBHOOKS SET userid = '%s' WHERE id = '%s'",
                        oldVsNewUser.get(entry.getValue()),
                        entry.getKey()));
            }
            exst(conn, String.format("UPDATE INCOMINGWEBHOOKS SET teamid = '%s'", targetTeamId));

            //Update name and userid in preferences
            updatePreferences(conn,oldVsNewUser);

            //Update channelmembers for userid
            updateChannelMembers(conn, oldVsNewUser);

            //Update creatorid in oauthapps - ignore zero rows
            //Update userid in oauthauthdata - ignore zero rows
            //Update userid, teamid in outgoingwebhooks - moved to delete


            //Delete other teams
            exst(conn, "DELETE FROM TEAMS WHERE id != '" + targetTeamId + "'");

            //Delete non target user memberships
            exst(conn, "DELETE FROM USERS WHERE teamid != '" + targetTeamId + "'");

            //Delete unwanted memberships
            exst(conn, "DELETE FROM CHANNELMEMBERS WHERE userid not in (SELECT id from USERS)");

            //Delete unwanted preferences
            exst(conn, "DELETE FROM PREFERENCES WHERE userid not in (SELECT id from USERS)");

            //Delete unwanted preferences
            exst(conn, "DELETE FROM OUTGOINGWEBHOOKS");

            //Clear all sessions and audits
            exst(conn, "DELETE FROM SESSIONS");
            exst(conn, "DELETE FROM AUDITS");

            //Misc
            exst(conn, String.format("UPDATE users SET roles = 'system_admin' where email = '%s'", cmd.getOptionValue("a")));

            //Add all users to town-square of Target team
            String channelId = getTownSquareOfTargetTeam(conn, targetTeamId);
            List<String> newMembers = getNewMembersOfTarget(conn, channelId);
            for (String newMember : newMembers)
            {
                exst(conn, String.format(
                        "INSERT INTO channelmembers VALUES('%s','%s','',0,0,0,'{\"desktop\":\"default\",\"mark_unread\":\"all\"}', 0)",
                        channelId, newMember));
            }


            //Write statements to file
            writetofile(statements);

            final List<Channel> newNonTargetPrivateChannels = getAllPrivateChannels(conn);
            for (Channel channel : newNonTargetPrivateChannels)
            {
                validateName(channel, conn);
            }

            //Validate creator id in channels
            //Validate teamid in channels
            //Validate userid in posts

        }
        catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static List<String> getNewMembersOfTarget(Connection conn, String channelId) throws SQLException
    {
        Statement st = conn.createStatement();
        final ResultSet rs = st.executeQuery(String.format("SELECT id, email FROM users WHERE id NOT IN " +
                "(SELECT userid FROM channelmembers WHERE channelid = '%s')", channelId));
        List<String> newMembers = new ArrayList<String>();
        while(rs.next())
        {
            newMembers.add(rs.getString("id"));
        }
        return newMembers;
    }

    private static String getTownSquareOfTargetTeam(Connection conn, String targetTeamId) throws SQLException
    {
        Statement st = conn.createStatement();
        final ResultSet rs = st.executeQuery(String.format("SELECT id FROM channels WHERE name = 'town-square' AND teamid = '%s'", targetTeamId));
        rs.next();
        return rs.getString("id");
    }

    private static void updateChannelMembers(Connection conn, Map<String, String> oldVsNewUser) throws SQLException
    {
        Statement st = conn.createStatement();
        final ResultSet rs = st.executeQuery("SELECT * FROM channelmembers");

        while(rs.next())
        {
            final String userid = rs.getString("userid");
            final String channelid = rs.getString("channelid");
            try
            {
                exst(conn, String.format("UPDATE channelmembers SET userid = '%s' WHERE userid = '%s' AND channelid = '%s'",
                        oldVsNewUser.get(userid), userid, channelid));
            }
            catch (Exception e)
            {
                System.out.println(e.getMessage());
            }
        }
    }

    private static void updatePreferences(Connection conn, Map<String, String> oldVsNewUser) throws SQLException
    {
        Statement st = conn.createStatement();
        final ResultSet rs = st.executeQuery("SELECT * FROM preferences");

        while(rs.next())
        {
            final String userid = rs.getString("userid");
            final String name = rs.getString("name");
            try
            {
                exst(conn, String.format("UPDATE PREFERENCES SET userid = '%s' WHERE userid = '%s' AND name = '%s'",
                        oldVsNewUser.get(userid), userid, name));
                exst(conn, String.format("UPDATE PREFERENCES SET name = '%s' WHERE userid = '%s' AND name = '%s'",
                        oldVsNewUser.get(name), userid, name));
            }
            catch (Exception e)
            {
                System.out.println(e.getMessage());
            }
        }
    }

    private static boolean areThereMatchingUserIds(String channelName, Map<String, String> oldVsNewUser)
    {
        final String[] split = channelName.split("__");
        return (oldVsNewUser.get(split[0]) != null) && (oldVsNewUser.get(split[1]) != null);
    }

    private static void validateName(Channel channel, Connection conn) throws SQLException
    {
        final String[] split = channel.getName().split("__");
        if(split[0].equals(split[1]))
        {
            System.out.println("Problem, same id in private channel");
        }
        else
        {
            printUserDetails(split[0], conn);
            printUserDetails(split[1], conn);
        }
    }

    private static void printUserDetails(String id, Connection conn) throws SQLException
    {
        Statement st = conn.createStatement();

        final ResultSet rs = st.executeQuery("SELECT users.id uid,users.email ue, users.username un, users.firstname ufn," +
                "users.teamid utid, teams.id tid, teams.name tn, teams.displayname tdn from USERS,TEAMS " +
                "WHERE USERS.teamid = TEAMS.id AND users.id = '" + id + "'");
        if(rs.next())
        {
            System.out.println(String.format("[%s] [%s] [%s] [%s] [%s] [%s] [%s] [%s] ",
                    rs.getString("ue"),
                    rs.getString("uid"),
                    rs.getString("un"),
                    rs.getString("ufn"),
                    rs.getString("utid"),
                    rs.getString("tn"),
                    rs.getString("tdn"),
                    rs.getString("ue")
            ));
        }
        else
        {
            System.out.println("User Not Found with ID " + id);
        }

    }

    private static String getExistingChannel(Connection conn, String newChannelName, String targetTeamId) throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT channels.id cid, channels.displayname cdn, " +
                "channels.name cn, channels.teamid ctid, channels.creatorid ccid," +
                "teams.name tn, teams.displayname tdn from CHANNELS,TEAMS where CHANNELS.teamid = TEAMS.id AND " +
                "channels.name = '" + newChannelName + "' AND " +
                "teams.id = '" + targetTeamId + "'");
        if(rs.next())
        {
            return rs.getString("cid");
        }
        else
        {
            return null;
        }
    }

    private static Map<String, String> getIncomingWebHooks(Connection conn) throws SQLException
    {
        Map<String,String> incomingWebHooks = new HashMap<String, String>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT id,userid FROM INCOMINGWEBHOOKS");
        while(rs.next())
        {
            incomingWebHooks.put(rs.getString("id"), rs.getString("userid"));
        }
        return incomingWebHooks;
    }

    private static String getNewChannelName(String currentName, Map<String, String> oldVsNewUser)
    {
        final String[] split = currentName.split("__");
        return oldVsNewUser.get(split[0]) + "__" + oldVsNewUser.get(split[1]);
    }

    private static List<Channel> getNonTargetPublicChannels(Connection conn, String targetTeamId) throws SQLException
    {
        List<Channel> data = new ArrayList<Channel>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT channels.id cid, channels.displayname cdn, " +
                "channels.name cn, channels.teamid ctid, channels.creatorid ccid," +
                "teams.name tn, teams.displayname tdn from CHANNELS,TEAMS where CHANNELS.teamid = TEAMS.id AND " +
                "channels.type = 'O' AND " +
                "channels.teamid != '" + targetTeamId + "'");
        while(rs.next())
        {
            data.add(new Channel(
                    rs.getString("cdn"),
                    rs.getString("cn"),
                    rs.getString("ctid"),
                    rs.getString("cid"),
                    rs.getString("ccid"),
                    rs.getString("tn"),
                    rs.getString("tdn"))
            );
        }
        return data;
    }

    private static List<Channel> getNonTargetDirectChannels(Connection conn, String targetTeamId) throws SQLException
    {
        List<Channel> data = new ArrayList<Channel>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT channels.id cid, channels.displayname cdn, " +
                "channels.name cn, channels.teamid ctid, channels.creatorid ccid," +
                "teams.name tn, teams.displayname tdn from CHANNELS,TEAMS where CHANNELS.teamid = TEAMS.id AND " +
                "channels.type = 'D' AND " +
                "channels.teamid != '" + targetTeamId + "'");
        while(rs.next())
        {
            data.add(new Channel(
                    rs.getString("cdn"),
                    rs.getString("cn"),
                    rs.getString("ctid"),
                    rs.getString("cid"),
                    rs.getString("ccid"),
                    rs.getString("tn"),
                    rs.getString("tdn"))
            );
        }
        return data;
    }

    private static List<Channel> getNonTargetPrivateChannels(Connection conn, String targetTeamId) throws SQLException
    {
        List<Channel> data = new ArrayList<Channel>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT channels.id cid, channels.displayname cdn, " +
                "channels.name cn, channels.teamid ctid, channels.creatorid ccid," +
                "teams.name tn, teams.displayname tdn from CHANNELS,TEAMS where CHANNELS.teamid = TEAMS.id AND " +
                "channels.type = 'P' AND " +
                "channels.teamid != '" + targetTeamId + "'");
        while(rs.next())
        {
            data.add(new Channel(
                    rs.getString("cdn"),
                    rs.getString("cn"),
                    rs.getString("ctid"),
                    rs.getString("cid"),
                    rs.getString("ccid"),
                    rs.getString("tn"),
                    rs.getString("tdn"))
            );
        }
        return data;
    }

    private static List<Channel> getAllPrivateChannels(Connection conn) throws SQLException
    {
        List<Channel> data = new ArrayList<Channel>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT channels.id cid, channels.displayname cdn, " +
                "channels.name cn, channels.teamid ctid, channels.creatorid ccid," +
                "teams.name tn, teams.displayname tdn from CHANNELS,TEAMS where CHANNELS.teamid = TEAMS.id AND " +
                "channels.type = 'D' ");
        while(rs.next())
        {
            data.add(new Channel(
                    rs.getString("cdn"),
                    rs.getString("cn"),
                    rs.getString("ctid"),
                    rs.getString("cid"),
                    rs.getString("ccid"),
                    rs.getString("tn"),
                    rs.getString("tdn"))
            );
        }
        return data;
    }
    private static List<String> statements = new ArrayList<String>();

    private static void exst(Connection conn, String statement) throws SQLException
    {
        System.out.println(statement + ";");
        statements.add(statement);
        final Statement stmt = conn.createStatement();
        stmt.execute(statement);
    }

    private static void writetofile(List<String> statements) throws IOException
    {
        FileWriter fstream = new FileWriter("/tmp/migrate.sql", true);
        BufferedWriter out = new BufferedWriter(fstream);
        for (String statement : statements)
        {
            out.write((statement + ";").toString());
            out.newLine();
        }

        //Close the output stream
        out.close();
    }

    private static Map<String, User> getExistingMembers(Connection conn, String targetTeamId) throws SQLException
    {
        Map<String, User> data = new HashMap<String, User>();
        Statement st = conn.createStatement();

        final ResultSet rs = st.executeQuery("SELECT users.id uid,users.email ue, users.username un, users.firstname ufn," +
                "users.teamid utid, teams.id tid, teams.name tn, teams.displayname tdn from USERS,TEAMS " +
                "WHERE USERS.teamid = TEAMS.id AND teamid = '" + targetTeamId + "'");
        while(rs.next())
        {
            data.put(rs.getString("ue"), new User(
                    rs.getString("uid"),
                    rs.getString("un"),
                    rs.getString("ufn"),
                    rs.getString("utid"),
                    rs.getString("tn"),
                    rs.getString("tdn"),
                    rs.getString("ue")
            ));
        }
        return data;
    }

    private static String getTargetTeamId(Connection conn, String targetTeamHandle) throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT id,name,displayname from TEAMS where name='" + targetTeamHandle + "'");
        rs.next();
        return rs.getString("id");
    }

    private static List<User> getAllUsers(Connection conn) throws SQLException
    {
        List<User> data = new ArrayList<User>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT users.id uid,users.email ue, users.username un, users.firstname ufn," +
                "users.teamid utid, teams.id tid, teams.name tn, teams.displayname tdn from USERS, TEAMS " +
                "WHERE USERS.teamid = TEAMS.id");
        while (rs.next())
        {
            data.add(new User(
                    rs.getString("uid"),
                    rs.getString("un"),
                    rs.getString("ufn"),
                    rs.getString("utid"),
                    rs.getString("tn"),
                    rs.getString("tdn"),
                    rs.getString("ue")
                    ));
        }
        rs.close();
        st.close();

        return data;
    }

    private static List<String> getDistinctEmailIDs(Connection conn) throws SQLException
    {
        List<String> data = new ArrayList<String>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT distinct(email) email from USERS");
        while (rs.next())
        {
            data.add(rs.getString("email"));
        }
        rs.close();
        st.close();

        return data;
    }
}
