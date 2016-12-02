package org.jumpmind.analytics;

public class ColumnData {

    private long id;

    private long rankHeaderId;

    private int type;

    private long valueTypeId;

    private int position;

    public long getId() {
        return id;
    }

    public void setId( long id ) {
        this.id = id;
    }

    public long getRankHeaderId() {
        return rankHeaderId;
    }

    public void setRankHeaderId( long rankHeaderId ) {
        this.rankHeaderId = rankHeaderId;
    }

    public int getType() {
        return type;
    }

    public void setType( int type ) {
        this.type = type;
    }

    public long getValueTypeId() {
        return valueTypeId;
    }

    public void setValueTypeId( long valueTypeId ) {
        this.valueTypeId = valueTypeId;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition( int position ) {
        this.position = position;
    }
}
