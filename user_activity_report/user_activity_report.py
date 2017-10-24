# -*- coding:utf-8  -*-

from base_models.wits_models import (Wits_user as users)
from projects import project

bke = project.Project('bke')
bke.configurate()
session = bke.sql_sessionmaker()
# test
user = session.query(users).filter_by(id=123).first()
print(user.name, user.last_name, user.first_name)
