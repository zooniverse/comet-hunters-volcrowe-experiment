#!/usr/bin/env python
__author__ = 'alex'
from collections import OrderedDict
from datetime import datetime, date, timedelta
import unicodecsv as csv
import sys
import os
import time
import json
import numpy
import matplotlib.pyplot as plt
import pickle

OUTLIER_LOW_CUTOFF = 1
OUTLIER_HIGH_CUTOFF = 100
NUMBER_OF_HISTOGRAM_BINS = 25

if len(sys.argv) > 1 and sys.argv[1]=="skip":
  skip_analysis=True
else:
  skip_analysis=False

def restart_line():
  sys.stdout.write('\r')
  sys.stdout.flush()

def get_user_session_classification_counts(classifications_by_user_session):
  counts_of_classifications_per_user_session = {}
  for user_session_id in classifications_by_user_session:
    counts_of_classifications_per_user_session[user_session_id] = len(classifications_by_user_session[user_session_id])
  return counts_of_classifications_per_user_session

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

def getWeekNumber(dateString):
  (y, m, d) = [int(i) for i in dateString.split("-")]
  return date(y,m,d).isocalendar()[1]

def averageLen(lst):
  lengths = [len(i) for i in lst]
  return 0 if len(lengths) == 0 else (float(sum(lengths)) / len(lengths))

def get_user_session_id(user_name, session):
  return "%s-%s" % (user_name, session)

def get_nice_now():
  return datetime.now().strftime('%H:%M:%S')

if not skip_analysis:

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
  classifications_by_user_session = {}
  skipped_due_to_no_session_set = 0
  classifications_by_day = {}
  classifications_by_week = {}
  classifications_by_user = {}
  users_by_day = {}
  users_by_week = {}

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
      if "session" in metadata:
        session_id = metadata["session"]
      else:
        skipped_due_to_no_session_set += 1
        skip_this_one = True
      if "finished_at" in metadata:
        finished_at = metadata["finished_at"][:10]
        date_of_this_classification = finished_at
        if date_of_this_classification in classifications_by_day:
          classifications_by_day[date_of_this_classification] += 1
        else:
          classifications_by_day[date_of_this_classification] = 1
        weekNum = getWeekNumber(finished_at)
        if weekNum in classifications_by_week:
          classifications_by_week[weekNum] += 1
        else:
          classifications_by_week[weekNum] = 1
        if user_name_field_index > -1:
          user_name = classification[user_name_field_index]
          if date_of_this_classification in users_by_day:
            if user_name not in users_by_day[date_of_this_classification]:
              users_by_day[date_of_this_classification].append(user_name)
          else:
            users_by_day[date_of_this_classification] = [user_name]
          if weekNum in users_by_week:
            if user_name not in users_by_week[weekNum]:
              users_by_week[weekNum].append(user_name)
          else:
            users_by_week[weekNum] = [user_name]
          if user_name not in classifications_by_user:
            classifications_by_user[user_name] = 1
          else:
            classifications_by_user[user_name] += 1
    if not skip_this_one and classification_id_field_index > -1:
      if user_name_field_index > -1:
        user_name = classification[user_name_field_index]
        user_session_id = get_user_session_id(user_name, session_id)
        if user_session_id not in classifications_by_user_session:
          classifications_by_user_session[user_session_id]=[]
        classification_id = classification[classification_id_field_index]
        classifications_by_user_session[user_session_id].append(classification_id)
    if classifications_analysed < total:
      sys.stdout.flush()

  print "\n\nProcessed a total of %s classifications (Finished at %s).\n" % (classifications_analysed, get_nice_now())

  print get_field_list(metadata_fields, metadata_field_index)

  skipped_pc = float(skipped_due_to_no_session_set)/float(total)

  print "Classifications skipped due to no session set: %s [%s%% of total]\n" % (skipped_due_to_no_session_set, skipped_pc)

  classification_session_counts = get_user_session_classification_counts(classifications_by_user_session)
  original_no_of_user_sessions = len(classification_session_counts)

  low_ones = 0
  for user_session_id in classification_session_counts.keys():
    if classification_session_counts[user_session_id] <= OUTLIER_LOW_CUTOFF:
      low_ones += 1
      del classification_session_counts[user_session_id]

  big_ones = 0
  for user_session_id in classification_session_counts.keys():
    if classification_session_counts[user_session_id] >= OUTLIER_HIGH_CUTOFF:
      big_ones += 1
      del classification_session_counts[user_session_id]

  average_classifications_per_user_session = numpy.mean(classification_session_counts.values())
  max_classifications_per_user_session = numpy.max(classification_session_counts.values())
  no_of_user_sessions = len(classification_session_counts)

  average_classifications_per_day = numpy.mean(classifications_by_day.values())
  average_classifications_per_week = numpy.mean(classifications_by_week.values())
  average_users_per_day = averageLen(users_by_day.values())
  average_users_per_week = averageLen(users_by_week.values())
  average_classifications_per_user = numpy.mean(classifications_by_user.values())

  print "Determined classification counts per user session for %s user sessions from an initial %s ..." % \
        (no_of_user_sessions, original_no_of_user_sessions)
  print " - %s had less than or equal to %s classification(s) and were deleted as outliers." % (low_ones,
                                                                                                OUTLIER_LOW_CUTOFF)
  print " - %s had equal to or more than %s classifications and were deleted as outliers." % (big_ones,
                                                                                              OUTLIER_HIGH_CUTOFF)
  print " - of those remaining, the maximum session length was %s." % max_classifications_per_user_session
  print " - of those remaining, the average session length was %s." % average_classifications_per_user_session

  print " - of those remaining, the average classifications per day was %s." % average_classifications_per_day
  print " - of those remaining, the average classifications per week was %s." % average_classifications_per_week
  print " - of those remaining, the average users per day was %s." % average_users_per_day
  print " - of those remaining, the average users per week was %s." % average_users_per_week
  print " - of those remaining, the average classifications per user was %s." % average_classifications_per_user

  print "\nSaving analysis to file..."
  pickle.dump([classification_session_counts,max_classifications_per_user_session], open('temp-data.p', 'wb'))

if 'classification_session_counts' not in vars() and 'max_classifications_per_user_session' not in vars():
  print "Loading analysis from last time..."
  [classification_session_counts, max_classifications_per_user_session] = pickle.load(open('temp-data.p', "rb"))

print "\nWriting histogram to graphs/session-length-histogram.png ..."
step = int(float(max_classifications_per_user_session) / float(NUMBER_OF_HISTOGRAM_BINS))
bins = numpy.arange(0, max_classifications_per_user_session, step)
session_lengths = classification_session_counts.values()
plt.hist(session_lengths, bins=bins)
plt.xticks(bins)
locs, labels = plt.xticks()
plt.setp(labels, rotation=90)
plt.xlabel('Session Length', fontsize=16)
plt.ylabel('Number of User Sessions of this Length', fontsize=16)
plt.savefig('graphs/session-length-histogram.png')
plt.clf()
plt.close()

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
