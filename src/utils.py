import os


def set_heroku_config_var(name, value):
	os.system('heroku config:set ' + name + '=' + value)