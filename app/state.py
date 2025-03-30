# app/state.py
import threading
 
class GlobalState(dict):
    """
    A thread-safe global state that behaves like a dictionary.
    Existing APIs that access state["table_names"], etc., will continue to work.
    """
    def __init__(self, *args, **kwargs):
        self._lock = threading.Lock()
        super().__init__(*args, **kwargs)
 
    def safe_clear(self):
        """
        Clears the state values in a thread-safe manner.
        Instead of deleting keys, we reset them to their initial values.
        """
        with self._lock:
            self["table_names"].clear()
            self["original_table_names"].clear()
            self["personal_engine"] = None
            self["mysql_connection"] = None
            self["chat_history"].clear()
 
# Instantiate the global state with default keys and values.
state = GlobalState({
    "table_names": [],           # List of tuples: (table_name, DataFrame)
    "original_table_names": [],  # List of tuples: (table_name, original DataFrame)
    "personal_engine": None,     # SQLAlchemy engine for personal DB
    "mysql_connection": None,    # MySQL connector connection if used
    "chat_history": []           # (Optional) Chat history if needed
})
 
 