/**
 * Created by Renjith Pillai (i306570) on 20/03/16.
 */
public class Channel
{
    public String getDisplayName()
    {
        return displayName;
    }

    public void setDisplayName(String displayName)
    {
        this.displayName = displayName;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getTeamId()
    {
        return teamId;
    }

    public void setTeamId(String teamId)
    {
        this.teamId = teamId;
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public String getCreatorId()
    {
        return creatorId;
    }

    public void setCreatorId(String creatorId)
    {
        this.creatorId = creatorId;
    }

    private String displayName;
    private String name;
    private String teamId;
    private String id;
    private String creatorId;
    private String teamName;
    private String teamDisplayName;

    public Channel(String displayName, String name, String teamId, String id, String creatorId, String teamName, String teamDisplayName)
    {
        this.displayName = displayName;
        this.name = name;
        this.teamId = teamId;
        this.id = id;
        this.creatorId = creatorId;
        this.teamName = teamName;
        this.teamDisplayName = teamDisplayName;
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
}
