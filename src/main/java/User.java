/**
 * Created by Renjith Pillai (i306570) on 20/03/16.
 */
public class User
{
    private String id;
    private String name;
    private String displayName;
    private String teamId;
    private String teamName;
    private String teamDisplayName;
    private String email;

    public User()
    {
    }

    public User(String id, String name, String displayName, String teamId, String teamName, String teamDisplayName, String email)
    {
        this.id = id;
        this.name = name;
        this.displayName = displayName;
        this.teamId = teamId;
        this.teamName = teamName;
        this.teamDisplayName = teamDisplayName;
        this.email = email;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getDisplayName()
    {
        return displayName;
    }

    public void setDisplayName(String displayName)
    {
        this.displayName = displayName;
    }

    public String getTeamId()
    {
        return teamId;
    }

    public void setTeamId(String teamId)
    {
        this.teamId = teamId;
    }

    public String getTeamName()
    {
        return teamName;
    }

    public void setTeamName(String teamName)
    {
        this.teamName = teamName;
    }

    public String getTeamDisplayName()
    {
        return teamDisplayName;
    }

    public void setTeamDisplayName(String teamDisplayName)
    {
        this.teamDisplayName = teamDisplayName;
    }

    public String getEmail()
    {
        return email;
    }

    public void setEmail(String email)
    {
        this.email = email;
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }
}
