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
 * Created by Renjith Pillai (i306570) on 18/03/16.
 */


public class Migrate
{
    private static final String TARGET_TEAM_ID = "ariba";

    public static void main(String ...args) throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        Class.forName("org.postgresql.Driver").newInstance();

        Connection conn = null;

        try {
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/i306570", "i306570", "ariba");


            //User updates
            final List<User> allUsers = getAllUsers(conn);
            final List<String> distinctEmailIDs = getDistinctEmailIDs(conn);
            final String targetTeamId = getTargetTeamId(conn);
            final Map<String, User> targetExistingMembers = getExistingMembers(conn, targetTeamId);

            for (String email :
                    distinctEmailIDs)
            {
                if (! targetExistingMembers.containsKey(email)) //Not Part of the target team
                {
                    User pickedUpUser = null;
                    for (User user : allUsers)
                    {
                        if(user.getEmail().equals(email))
                        {
                            if(null == pickedUpUser)
                            {
                                //This means, we are just finding the user to be migrated into target team
                                pickedUpUser = user;
                                exst(String.format("UPDATE USERS SET teamid = '%s' WHERE id = '%s'", targetTeamId, user.getId()));
                                pickedUpUser.setTeamId(targetTeamId);//Just for having correct data
                            }
                            else
                            {
                                //We already picked up one. Swap all the posts to this user
                                exst(String.format("UPDATE POSTS SET userid = '%s' WHERE userid = '%s'", pickedUpUser.getId(), user.getId()));
                            }
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
                            if(! pickedUpUser.getId().equals(user.getId()))
                            {
                                exst(String.format("UPDATE POSTS SET userid = '%s' WHERE userid = '%s'", pickedUpUser.getId(), user.getId()));
                            }
                            else
                            {
                                System.out.println("We know this");
                            }
                        }
                    }
                }
            }

            //Update non target public channels to target team id
            final List<Channel> publicChannels = getNonTargetPublicChannels(conn, targetTeamId);
            for(Channel channel : publicChannels)
            {
                exst(String.format("UPDATE CHANNELS SET displayname = '%s-%s' AND name = '%s-%s' AND teamid = '%s' WHERE id = '%s'",
                        channel.getTeamDisplayName(),channel.getDisplayName(),
                        channel.getTeamName(),channel.getName(), targetTeamId,
                        channel.getId()));
            }

        }
        catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
    }

    private static List<Channel> getNonTargetPublicChannels(Connection conn, String targetTeamId) throws SQLException
    {
        List<Channel> data = new ArrayList<Channel>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT channels.id cid, channels.displayname cdn, " +
                "channels.name cn, channels.teamid ctid, channels.creatorid ccid," +
                "teams.name tn, teams.displayname tdn from CHANNELS,TEAMS where CHANNELS.teamid = TEAMS.id AND " +
                "channels.displayname != '' AND " +
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

    private static void exst(String statement)
    {
        System.out.println(statement);
    }

    private static Map<String, User> getExistingMembers(Connection conn, String targetTeamId) throws SQLException
    {
        Map<String, User> data = new HashMap<String, User>();
        Statement st = conn.createStatement();

        final ResultSet rs = st.executeQuery("SELECT users.id uid,users.email ue, users.name un, users.displayname udn," +
                "users.teamid utid, teams.id tid, teams.name tn, teams.displayname tdn from USERS " +
                "WHERE USERS.teamid = TEAMS.id AND teamid = '" + targetTeamId + "'");
        while(rs.next())
        {
            data.put(rs.getString("ue"), new User(
                    rs.getString("uid"),
                    rs.getString("un"),
                    rs.getString("udn"),
                    rs.getString("utid"),
                    rs.getString("tn"),
                    rs.getString("tdn"),
                    rs.getString("ue")
            ));
        }
        return data;
    }

    private static String getTargetTeamId(Connection conn) throws SQLException
    {
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT id,name,displayname from TEAMS where name='" + TARGET_TEAM_ID + "'");
        rs.next();
        return rs.getString("id");
    }

    private static List<User> getAllUsers(Connection conn) throws SQLException
    {
        List<User> data = new ArrayList<User>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT users.id uid,users.email ue, users.name un, users.displayname udn," +
                "users.teamid utid, teams.id tid, teams.name tn, teams.displayname tdn from USERS " +
                "WHERE USERS.teamid = TEAMS.id");
        while (rs.next())
        {
            data.add(new User(
                    rs.getString("uid"),
                    rs.getString("un"),
                    rs.getString("udn"),
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
