import requests
from lxml import html
import re
import pymysql.cursors
import logging
import time
import datetime

logging.basicConfig(filename='logfile/logger.log', level=logging.INFO)
base_url = 'https://www.wsl.waseda.jp/syllabus/JAA104.php?pKey='
dot = '= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ='
connection = pymysql.connect(host='localhost', user='dbuser', password='ongydbpass', db='103', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

def logger(text):
  print(f'\n{text}' if dot in text else text)
  logging.info(text)

def get_timestamp():
  ts = time.time()
  timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
  return timestamp

def get_nodes():
  try:
    with connection.cursor() as cursor:
      sql = 'SELECT * FROM nodes WHERE id = 2'
      logger(sql)
      cursor.execute(sql)
      return cursor.fetchall()
  except:
    logger('There was a problem getting nodes.')
      
def commit_data(left, right):
  try:
    with connection.cursor() as cursor:
      sql = f'INSERT INTO {left} VALUES {right}'
      logger(sql)
      cursor.execute(sql)
      connection.commit()
      logger('Successfully commited data.')
  except:
    logger('There was a problem commiting data.')
    
def select_one(table, options=''):
  try:
    with connection.cursor() as cursor:
      sql = f'SELECT id FROM {table} {options}'
      logger(sql)
      cursor.execute(sql)
      r = cursor.fetchall()
      return r[0]['id'] if r else r
  except:
    logger(f'There was a problem selecting data. Table: {table}')
    
def last_row_id():
  try:
    with connection.cursor() as cursor:
      cursor.execute('SELECT last_insert_id()')
      r = cursor.fetchone()
      return r['last_insert_id()']
  except:
    logger('There was a problem returning last row id.')

def get_data(x, lng):
  response = requests.get(f'{base_url}{x}&pLng={lng}')
  return html.fromstring(response.content)

def is_empty(x):
  if len(x) == 1: return True if ord(x) == 32 or ord(x) == 160 else False
  return False

def parse(x):
  return x[0].text_content().replace('　', ' ').replace('／', '/').replace(u'\xa0', u' ') if x else x

def path(name, lng):
  return parse(lng.xpath(f'//th[text()="{name}"]/following-sibling::td'))

def multiple_path(name, lng):
  k = []
  x = lng.xpath(f'//th[text()="{name}"]/following-sibling::td')
  for o in x: k.append(o.text_content())
  return k

def reg_exp(expression, text):
  result = re.search(expression, text).group()
  if expression == r'\d+': return int(result)
  return result

def get_initial_data(x, jp, en):
  
  def get_dept_id(x):
    r = select_one('depts', f'WHERE en_long = "{x}"')
    return r if r else 15
    
  def get_language_id(x):
    r = select_one('languages', f'WHERE en = "{x}"')
    return r if r else 16
    
  def get_term_id(x):
    if 'and' in x: return 10
    s = ['spring', 'summer', 'fall', 'winter', 'full']
    q = ['semester', 'quarter', 'year']
    r = [i for i in s if i in x]
    r2 = [l for l in q if l in x]
    if r2: d = select_one('terms', f'WHERE en = "{r[0]} {r2[0]}"')
    else: d = select_one('terms', f'WHERE en = "{r[0]}"')
    return d if d else 10
    
  def get_campus_id(x):
    r = select_one('campuses', f'WHERE en = "{x}"')
    return r if r else 6
  
  year = reg_exp(r'\d+', path('Year', en))
  dept = get_dept_id(path('School', en))
  title_jp = path('科目名', jp)
  title_en = path('Course Title', en)
  credit = reg_exp(r'\d+', path('Credits', en))
  eligible = reg_exp(r'\d+', path('Eligible Year', en))
  url = x
  category_jp = path('科目区分', jp)
  category_en = path('Category', en)
  language = get_language_id(path('Main Language', en))
  term = get_term_id(path('Term/Day/Period', en))
  campus = get_campus_id(path('Campus', en))
  open = 1 if path('Open Courses', en) else 0
  table = 'classes_test(year, dept_id, title_jp, title_en, credit, eligible, url, category_jp, category_en, language_id, term_id, campus_id, is_open, created_at, updated_at)'
  values = (year, dept, title_jp, title_en, credit, eligible, url, category_jp, category_en, language, term, campus, open, get_timestamp(), get_timestamp())
  commit_data(table, values)
  return last_row_id()

def get_sub_data(id, jp, en):
  
  def ditch(x):
    w = ['spring', 'summer', 'fall', 'winter', 'semester', 'quarter', 'full', 'year', 'and', 'demand']
    om = [o for o in x.split(' ') if not o in w and o]
    return om[0] if len(om) == 1 else om[-1]
  
  def get_class_cats(id, en):
    codes = multiple_path('Course Code', en)
    for x in codes:
      if not is_empty(x):
        first = select_one('firsts', f'WHERE code = "{x[0:3]}"')
        second = select_one('seconds', f'WHERE code = "{x[3]}" AND first_id = {first}')
        level = 'Z' if x[4] == 'Z' else select_one('levels', f'WHERE id = {x[4]}')
        third = select_one('thirds', f'WHERE code = "{x[5]}" AND first_id = {first} AND second_id = {second}')
        number = x[6]
        type = select_one('types', f'WHERE code = "{x[-1]}"')
        table = 'class_cats_test (class_id, first_id, second_id, third_id, level_id, course_number, type_id, created_at, updated_at)'
        values = (id, first, second, third, level, number, type, get_timestamp(), get_timestamp())
        commit_data(table, values)
  
  def get_periods(id, en):
    ww = ['Sun', 'Mon', 'Tues', 'Wed', 'Thur', 'Fri', 'Sat', 'others', 'Othersothers', 'demand', 'othersothers']
    table = 'periods_test (class_id, `order`, day, period, created_at, updated_at)'
    
    def make_values(id, x, order=1):
      ap = x.split('.')
      day = ww.index(ap[0]) if ap[0] in ww else 7
      if len(ap) > 1 and '-' in ap[1]:
        a = ap[1].split('-')
        for i in a:
          values = (id, order, day, i, get_timestamp(), get_timestamp())
          commit_data(table, values)
      elif len(ap) > 1 and not is_empty(ap[1]):
        values = (id, order, day, ap[1], get_timestamp(), get_timestamp())
        commit_data(table, values)
      else:
        values = (id, order, day, 0, get_timestamp(), get_timestamp())
        commit_data(table, values)
    
    tr = ditch(path('Term/Day/Period', en))
    if '/' in tr:
      sp = tr.split('/')
      for e in sp:
        li = e.split(':')
        make_values(id, li[1], int(li[0]))
    elif '.' in tr:
      make_values(id, tr)
    else:
      values = (id, 1, 7, 0, created_at, updated_at)
      commit_data(table, values)
      
  def get_class_prof(id, jp, en):
    
    def check_existance(jp, en):
      d = select_one('profs_test', f'WHERE name_jp = "{jp}"')
      if d: return d
      else:
        commit_data('profs_test (name_jp, name_en, created_at, updated_at)', (jp, en, get_timestamp(), get_timestamp()))
        return last_row_id()
    
    pjp = path('担当教員', jp).replace(' 他', '').split('/')
    pen = path('Instructor', en).replace(' others', '').split('/')
    table = 'class_profs_test (class_id, prof_id, created_at, updated_at)'
    for j in pjp:
      i = check_existance(j, pen[pjp.index(j)])
      values = (id, i, get_timestamp(), get_timestamp())
      commit_data(table, values)
  
  def get_class_room(id, en):
    
    def make_values(id, c, x, order=1):
      table = 'class_rooms_test (order, class_id, room_id, created_at, updated_at)'
      ap = x.split('-')
      av = select_one('rooms', f'WHERE campus_id = {c} AND building = {ap[0]} AND room = "{ap[1]}"')
      if av:
        commit_data(table, (order, id, av, get_timestamp(), get_timestamp()))
      else:
        commit_data('rooms (campus_id, building, room, created_at, updated_at)', (c, ap[0], ap[1], get_timestamp(), get_timestamp()))
        i = last_row_id()
        commit_data(table, (order, id, i, get_timestamp(), get_timestamp()))
      
    tr = path('Classroom', en)
    e = path('Campus', en)
    r = select_one('campuses', f'WHERE en = "{e}"')
    c = r if r else 6
    if '/' in tr:
      sp = tr.split('/')
      for e in sp:
        li = e.split(':')
        make_values(id, c, li[1], int(li[0]))
    elif '-' in tr:
      make_values(id, c, tr)
    else:
      logger('Classroom is empty' if is_empty(tr) else f'Found an eception in classroom name: {tr}')
  
  def get_detail_and_grading(id, en):
    
    def get_grading_id(en):

      def gon(name, lng):
        x = lng.xpath(f'//td[text()="{name}:"]/following-sibling::td')
        return re.search(r'\d+', x[0].text_content()).group() if x else 0
      
      raw = path('Evaluation', en)
      if raw and not is_empty(raw):
        parsable = 1 if '\r\nRate\r\nEvaluation Criteria\r\n' in raw else 0
        table = 'gradings_test (class_id, exam, paper, attendance, others, can_parse, raw, created_at, updated_at)'
        values = (id, gon("Exam", en), gon("Papers", en), gon("Class Participation", en), gon("Others", en), parsable, raw, get_timestamp(), get_timestamp())
        commit_data(table, values)
        return last_row_id()
      return 0
    
    def ifer(x):
      return x if x else 'none'
    
    subtitle = path('Subtitle', en)
    outline = path('Course Outline', en)
    objective = path('Objectives', en)
    beforeafter = path('before/after course of study', en)
    schedule = path('Course Schedule', en)
    textbook = path('Textbooks', en)
    reference = path('Reference', en)
    i = get_grading_id(en)
    note = path('Note / URL', en)
    table = 'details (class_id, subtitle, outline, objective, beforeafter, schedule, textbook, reference, grading_id, note, created_at, updated_at)'
    values = (id, ifer(subtitle), ifer(outline), ifer(objective), ifer(beforeafter), ifer(schedule), ifer(textbook), ifer(reference), i, ifer(note), get_timestamp(), get_timestamp())
    commit_data(table, values)
  
  get_class_cats(id, en)
  get_periods(id, en)
  get_class_prof(id, jp, en)
  get_class_room(id, en)
  get_detail_and_grading(id, en)

def main():
  data = get_nodes()
  for item in data:
    url = item['url']
    jp = get_data(url, 'jp')
    en = get_data(url, 'en')
    logger(f'{dot} ID: {url} {dot}')
    init_data = get_initial_data(url, jp, en)
    get_sub_data(init_data, jp, en)
    
main()
connection.close()