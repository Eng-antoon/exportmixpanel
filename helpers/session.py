from flask import session as flask_session

def get_saved_filters():
    return flask_session.get("saved_filters", {})

def save_filter_to_session(name, filters):
    saved = flask_session.get("saved_filters", {})
    saved[name] = filters
    flask_session["saved_filters"] = saved
