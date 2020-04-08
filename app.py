import scrapy
import pymysql
import requests
from lxml import html
import re

# predefining
db = pymysql.connect('localhost', 'dbuser', 'ongydbpass', '103')
cursor = db.cursor()

baseurl = 'https://www.wsl.waseda.jp/syllabus/JAA104.php?pKey='
# stmt = 'SELECT * FROM nodes WHERE id < 100 OR id > 1435 AND id < 1536 OR id > 2801 AND id < 3200 OR id > 5100 AND id < 5200 OR id > 6337 AND id < 6437 OR id > 7445 AND id < 7545 OR id > 8320 AND id < 8420 OR id > 8948 AND id < 9048 OR id > 9366 AND id < 9466 OR id > 11576 AND id < 11676'
# cursor.execute(stmt)
cursor.execute('SELECT * FROM nodes WHERE id > 11171')
data = cursor.fetchall()

def get_data(item, lng):
  url = f'{baseurl}{item[3]}&pLng={lng}'
  response = requests.get(url)
  doc = html.fromstring(response.content)
  return doc
  
def parse(x):
  if x: return x[0].text_content().replace('　', ' ').replace('／', '/').replace(u'\xa0', u' ')
  
def basic_information(jp, en, item):
  def get_language_id(x):
    cursor.execute(f'SELECT * FROM languages WHERE en = "{x}"')
    langItem = cursor.fetchone()
    if langItem:
      return langItem[0]
    else:
      print('this language is not registered yet: ' + x)
      return 16
    
  def get_term_id(x):
    splitted = x.split(' ')
    text = f'{splitted[0].capitalize()} {splitted[1]}' if splitted[1] != '' else splitted[0].capitalize()
    cursor.execute(f'SELECT * FROM terms WHERE en = "{text}"')
    termItem = cursor.fetchone()
    if termItem:
      return termItem[0]
    else:
      return 9
  
  def get_dept_id(x):
    cursor.execute(f'SELECT * FROM depts WHERE en_long = "{x}"')
    deptItem = cursor.fetchone()
    return deptItem[0]
    
  def get_campus_id(x):
    cursor.execute(f'SELECT * FROM campuses WHERE en = "{x}"')
    campusItem = cursor.fetchone()
    return campusItem[0]
  
  year = int(re.search('^[1-3][0-9]{3}', parse(en.xpath('//th[text()="Year"]/following-sibling::td'))).group())
  dept = get_dept_id(parse(en.xpath('//th[text()="School"]/following-sibling::td')))
  title_jp = parse(jp.xpath('//th[text()="科目名"]/following-sibling::td'))
  title_en = parse(en.xpath('//th[text()="Course Title"]/following-sibling::td'))
  credit = parse(en.xpath('//th[text()="Credits"]/following-sibling::td'))
  eligible = parse(en.xpath('//th[text()="Eligible Year"]/following-sibling::td'))[0]
  url = item[3]
  category_jp = parse(jp.xpath('//th[text()="科目区分"]/following-sibling::td'))
  category_en = parse(en.xpath('//th[text()="Category"]/following-sibling::td'))
  language = get_language_id(parse(en.xpath('//th[text()="Main Language"]/following-sibling::td')))
  term = get_term_id(parse(en.xpath('//th[text()="Term/Day/Period"]/following-sibling::td')))
  campus = get_campus_id(parse(en.xpath('//th[text()="Campus"]/following-sibling::td')))
  open = 1 if parse(en.xpath('//td[text()="Open Courses"]')) else 0
  values = (year, dept, title_jp, title_en, credit, eligible, url, category_jp, category_en, language, term, campus, open)
  stmt = f'INSERT INTO classes_test(year, dept_id, title_jp, title_en, credit, eligible, url, category_jp, category_en, language_id, term_id, campus_id, is_open) VALUES {values}'
  cursor.execute(stmt)
  db.commit()
  return [cursor.lastrowid, campus]

def add_class_cats(id, en):
  code = en.xpath('//th[text()="Course Code"]/following-sibling::td')
  if code[0].text_content() != '&nbsp;':
    def get_first(code):
      stmt = f'SELECT id FROM firsts WHERE code = "{code}"'
      cursor.execute(stmt)
      return cursor.fetchone()
    
    def get_second(first, code):
      stmt = f'SELECT id FROM seconds WHERE code = "{code}" AND first_id = {first}'
      cursor.execute(stmt)
      return cursor.fetchone()
    
    def get_third(first, second, code):
      stmt = f'SELECT id FROM thirds WHERE code = "{code}" AND first_id = {first} AND second_id = {second}'
      cursor.execute(stmt)
      return cursor.fetchone()
    
    def get_type(code):
      stmt = f'SELECT id FROM types WHERE code = "{code}"'
      cursor.execute(stmt)
      return cursor.fetchone()
      
    for x in code:
      text = x.text_content()
      first = get_first(text[0:3])[0]
      second = get_second(first, text[3])[0]
      level = text[4] if text[4] != 'Z' else 8
      third = get_third(first, second, text[5])[0]
      number = text[6]
      type = get_type(text[-1])[0]
      values = (id, first, second, third, level, number, type)
      stmt = f'INSERT INTO class_cats_test (class_id, first_id, second_id, third_id, level_id, course_number, type_id) VALUES {values}'
      cursor.execute(stmt)
      db.commit()

def add_periods(id, en):
  waseda_week = ['Sun', 'Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat', 'others']
  target = parse(en.xpath('//th[text()="Term/Day/Period"]/following-sibling::td'))
  end = target.split(' ')[-1]
  if '/' in end:
    apart = end.split('/')
    for x in apart:
      apt = x.split(':')
      two = apt[1].split('.')
      safe = waseda_week.index(two[0]) if two[0] != 'othersothers' else 9
      two[1] = two[1] if re.search(r'\d+', two[1]) else '99'
      safe_two = int(two[1]) if len(two) == 2 else 9
      values = (id, int(apt[0]), safe, safe_two)
      stmt = f'INSERT INTO periods_test (class_id, `order`, day, period) VALUES {values}'
      cursor.execute(stmt)
      db.commit()
  elif '.' in end:
    if '-' in end:
      apart = end.split('.')
      index = waseda_week.index(apart[0])
      days = apart[1].split('-')
      for x in days:
        values = (id, 1, index, int(x))
        stmt = f'INSERT INTO periods_test (class_id, `order`, day, period) VALUES {values}'
        cursor.execute(stmt)
        db.commit()
    else:
      apart = end.split('.')
      index = waseda_week.index(apart[0])
      values = (id, 1, index, int(apart[1]))
      stmt = f'INSERT INTO periods_test (class_id, `order`, day, period) VALUES {values}'
      cursor.execute(stmt)
      db.commit()
  else:
    print('ondemand probably')

def add_class_prof(id, jp, en):
  profs_jp = parse(jp.xpath('//th[text()="担当教員"]/following-sibling::td')).split('/')
  profs_en = parse(en.xpath('//th[text()="Instructor"]/following-sibling::td')).split('/')
  for x in profs_jp:
    jp = x 
    en = profs_en[profs_jp.index(x)]
    cursor.execute(f'SELECT id FROM profs_test WHERE name_jp = "{x}"')
    em = cursor.fetchone()
    if not em:
      val = (jp, en)
      cursor.execute(f'INSERT INTO profs_test (name_jp, name_en) VALUES {val}')
      db.commit()
      back = cursor.lastrowid
      values = (id, back)
      cursor.execute(f'INSERT INTO class_profs_test (class_id, prof_id) VALUES {values}')
      db.commit()
    else:
      values = (id, em[0])
      cursor.execute(f'INSERT INTO class_profs_test (class_id, prof_id) VALUES {values}')
      db.commit()

def add_class_room(id, en):
  target = parse(en.xpath('//th[text()="Classroom"]/following-sibling::td'))
  
  def check_existance(values):
    cursor.execute(f'SELECT id FROM rooms WHERE campus_id = {values[0]} AND building = {values[1]} AND room = "{values[2]}"')
    detr = cursor.fetchone()
    if detr:
      return detr[0]
    else:
      cursor.execute(f'INSERT INTO rooms (campus_id, building, room) VALUES {values}')
      db.commit()
      return cursor.lastrowid
  
  if '/' in target:
    apart = target.split('/')
    for x in apart:
      two = x.split(':')
      em = two[1].split('-')
      if len(em) > 1:
        val = (id[1], int(em[0]), em[1])
        room_id = check_existance(val)
        cursor.execute(f'INSERT INTO class_rooms_test (`order`, class_id, room_id) VALUES (1, {id[0]}, {room_id})')
        db.commit()
  elif '-' in target:
    em = target.split('-')
    val = (id[1], int(em[0]), em[1])
    room_id = check_existance(val)
    cursor.execute(f'INSERT INTO class_rooms_test (`order`, class_id, room_id) VALUES (1, {id[0]}, {room_id})')
    db.commit()

def add_detail_and_grading(id, en):
  
  def gon(x):
    if x:
      exist = re.search(r'\d+', x)
      if exist:
        return exist.group()
      else:
        return 0
    else:
      return 0
  
  def get_grading(en):
    exam = parse(en.xpath('//td[text()="Exam:"]/following-sibling::td'))
    paper = parse(en.xpath('//td[text()="Papers:"]/following-sibling::td'))
    attendance = parse(en.xpath('//td[text()="Class Participation:"]/following-sibling::td'))
    others = parse(en.xpath('//td[text()="Others:"]/following-sibling::td'))
    return [gon(exam), gon(paper), gon(attendance), gon(others)]
  
  def check_parseable(text):
    safe = text.split('\n')
    if len(safe):
      result = 1 if safe[0] == '\r' else 0
      return result
    else:
      return 0
    
  gradings = get_grading(en)
  raw = parse(en.xpath('//th[text()="Evaluation"]/following-sibling::td'))
  can_parse = check_parseable(raw)
  values = (gradings[0], gradings[1], gradings[2], gradings[3], can_parse, raw)
  cursor.execute(f'INSERT INTO gradings_test (exam, paper, attendance, others, can_parse, raw) VALUES {values}')
  db.commit()

def sub_tables(jp, en, id):
  add_class_cats(id[0], en)
  add_periods(id[0], en)
  add_class_prof(id[0], jp, en)
  add_class_room(id, en)
  add_detail_and_grading(id, en)

for item in data:
  jp = get_data(item, 'jp')
  en = get_data(item, 'en')
  init_id = basic_information(jp, en, item)
  sub_tables(jp, en, init_id)
  print('done: ' + item[3])
  