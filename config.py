from configparser import ConfigParser


def api_config(filename="api.ini", section="api"):
    parser = ConfigParser()
    parser.read(filename)
    api = {}
    params = parser.items(section)
    for param in params:
        api[param[0]] = param[1]
    return api


def ssh_config(filename="ssh.ini", section="ssh"):
    parser = ConfigParser()
    parser.read(filename)
    ssh = {}
    params = parser.items(section)
    for param in params:
        ssh[param[0]] = param[1]
    return ssh


def db_config(filename="database.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    params = parser.items(section)
    for param in params:
        db[param[0]] = param[1]
    return db
