#!/usr/bin/env python
__author__ = 'alex'
from collections import OrderedDict
from datetime import datetime, timedelta
import unicodecsv as csv
import sys
import os
import time
import json
import numpy

def restart_line():
  sys.stdout.write('\r')
  sys.stdout.flush()

def get_user_classification_counts(classifications_by_user):
  counts_of_classifications_per_user = {}
  for user in classifications_by_user:
    counts_of_classifications_per_user[user] = len(classifications_by_user[user])
  return counts_of_classifications_per_user

def get_headers_with_indices(headers):
  s = "Available columns:\n"
  i=0
  for v in headers:
    s += "  %s: %s\n" % (i, v)
    i += 1
  return s

def get_field_list(field_array, column_index):
  s = "Available metadata fields found (in JSON string in column %s):\n" % column_index
  for v in field_array:
    s += "  %s\n" % v
  return s

def get_user_session_id(user_name, session):
  return "%s-%s" % (user_name, session)

def get_nice_now():
  return datetime.now().strftime('%H:%M:%S')

print "\nScanning classifications CSV (started at %s)...\n" % get_nice_now()

classifications_analysed = 0
filename = 'data/comet-hunters-classifications.csv'
total = sum(1 for line in open(filename)) - 1
classifications = csv.reader(open(filename, 'rU'), dialect=csv.excel_tab, delimiter=',', quotechar='"')
headers = classifications.next()

metadata_field_index = headers.index("metadata")
user_name_field_index = headers.index("user_name")
classification_id_field_index = headers.index("classification_id")

print get_headers_with_indices(headers)

metadata_fields = []
classifications_by_user = {}
skipped_due_to_no_session_set = 0

print "Total classifications (data rows) in CSV: %s\n" % total

for classification in classifications:
  skip_this_one = False
  classifications_analysed += 1
  if classifications_analysed % 1000 == 0:
    restart_line()
    pc = int(100*(float(classifications_analysed)/float(total)))
    sys.stdout.write("%s - %s classifications examined (%s%%)..." % (get_nice_now(), classifications_analysed, pc))
  if metadata_field_index > 0:
    metadata = json.loads(classification[metadata_field_index])
    for field in metadata:
      if field not in metadata_fields:
        metadata_fields.append(field)
    if "session" not in metadata:
      skipped_due_to_no_session_set += 1
      skip_this_one = True
  if not skip_this_one and classification_id_field_index > -1:
    if user_name_field_index > -1:
      user_name = classification[user_name_field_index]
      if user_name not in classifications_by_user:
        classifications_by_user[user_name]=[]
      classification_id = classification[classification_id_field_index]
      classifications_by_user[user_name].append(classification_id)
  if classifications_analysed < total:
    sys.stdout.flush()
  #day_of_this_classification = datetime.strptime(classification[4].split(' ')[0], '%Y-%m-%d')

print "\n\nProcessed a total of %s classifications (Finished at %s).\n" % (classifications_analysed, get_nice_now())

print get_field_list(metadata_fields, metadata_field_index)

skipped_pc = float(skipped_due_to_no_session_set)/float(total)

print "\nClassifications skipped due to no session set: %s [%s%% of total]\n" % (skipped_due_to_no_session_set, skipped_pc)

classification_counts = get_user_classification_counts(classifications_by_user)
max_classifications_per_user = numpy.max(classification_counts.values())

original_no_of_users = len(classification_counts)

oners = 0
for user in classification_counts.keys():
  if classification_counts[user] == 1:
    oners += 1
    #del classification_counts[user]

average_classifications_per_user = numpy.mean(classification_counts.values())

no_of_users = len(classification_counts)

print "Determined classification counts per user for %s users (of which %s only did 1): " \
      "maximum %s, average %s classifications made over all time." % (no_of_users, oners,
                                                       max_classifications_per_user,
                                                       average_classifications_per_user)



#wrfile = open("output/subjects_activity.csv", 'w')

#writer = csv.writer(wrfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC, dialect='excel', encoding='utf-8')

#writer.writerow(["Subject ID", "First Classification ID", "First Classification Date", "Activity Days"])
# write the subject activity to a csv file
#for subject in subject_activity:
#  row = [subject, subject_activity[subject]["first_classification_id"],
#         subject_activity[subject]["first_classification_date"],
#         ','.join([i.strftime('%d-%b-%Y') for i in subject_activity[subject]["active_days"]])]
#  outrow = []
#  for el in row:
#    if isinstance(el, str):
#      outrow.append(unicode(el.decode('utf-8')))
#    else:
#      outrow.append(el)
#  writer.writerow(outrow)
#wrfile.close()

print "\nDone.\n"
