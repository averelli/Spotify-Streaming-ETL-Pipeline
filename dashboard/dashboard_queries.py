import pandas as pd
from scripts.connectors.db_manager import DatabaseManager

def get_chart_data(db:DatabaseManager, item_type:str, year:int=None, month:int=None, limit:int=5):
    """
    Returns top n items for specified period or all time data by default.

    Params:
        db (DatabaseManager): DatabaseManager instance
        item_type (str): `album`, `track` or `artist`
        year (int): Specified year, None by defaut
        month (int): Specified month within the specified year. Only valid if used with the year filter. None by default
        limit (int): How many rows to return. 10 by default
    
    Returns:
        pd.DataFrame with the results
    """

    if item_type not in ["album", "track", "artist"]:
        raise ValueError("item_type only accepts album, track or artist")
    
    second_col = f"{item_type}_artist" if item_type != "artist" else None
    columns = [item_type, second_col, "hours_played", "times_played", "estimated_streams", "full_real_steams", "cover_art"]
    
    data = db.execute_query(f"SELECT * FROM dm.top_{item_type}s(%s, %s, %s);", (year, month, limit))

    return pd.DataFrame(data, columns=[x for x in columns if x])

def get_aggregated_data(db:DatabaseManager, grain:str):
    """
    Returns data from the aggregated views.

    Args:
        db (DatabaseManager): db instance
        fiter (str): `year` or `month` 

    Returns:
        list: Data from the view
    """
    if grain not in ["year", "month"]:
        raise ValueError(f"Grain value can only be month or year. {grain} passed instead.")
    
    return db.execute_query(f"SELECT * FROM dm.{grain}ly_agg;")

