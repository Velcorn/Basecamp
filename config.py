from configparser import ConfigParser


def dbconfig(filename="database.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    params = parser.items(section)
    for param in params:
        db[param[0]] = param[1]
    return db


def sshconfig(filename="ssh.ini", section="ssh"):
    parser = ConfigParser()
    parser.read(filename)
    ssh = {}
    params = parser.items(section)
    for param in params:
        ssh[param[0]] = param[1]
    return ssh
