#
# Copyright(c) 2021 by International Business Machines, Inc. All rights reserved.
# This code is provided AS-IS. 
#
#
# delete_all_documents.py: deletes all documents from a Watson Discovery V2 collection
# 		without deleting the collection. This script uses the authentication used in 
#       cp4d deployments.
#
# Usage:
#   python3 delete_all_documents.py PROJECT_ID COLLECTION_ID
#
#   The script will query the collection's index for the document_ids of the documents
#   in the collection and then use the DELETE v2/documents API to delete those documents.
#   The script will keep pooling the index and will stop when it detected that the 
#   documents are gone from the index.
#
#   The script assumes the following environment variables are set:
#     WD_URL:   the Watson Discovery instance API URL
#     WD_TOKEN: the Watson Discovery instance API TOKEN 
#
#   This script is intended to be used in CP4D instances (not cloud). If you want to use
#   it in cloud, you need to change the Authenticator used.
#
# Needs:
#   ibm_watson: See https://cloud.ibm.com/apidocs/discovery-data?code=python#introduction
#   requests:   pip3 install requests
#
# Remarks:
#   This script will delete all data from the collection of the provided collection_id. 
#   This is not a reversible operation. Make sure you have a backup of the data that
#   was in the collection before you run. The script will not ask you to confirm the 
#   deletes - please ensure that you are using the correct collection id.
#
#   This script is provided AS-IS. Use at your own risk. 
#
import os
import sys
import requests
import time
from urllib3.exceptions import InsecureRequestWarning
from ibm_watson import DiscoveryV2
from ibm_cloud_sdk_core.authenticators import BearerTokenAuthenticator
from concurrent.futures import ThreadPoolExecutor

if len(sys.argv) < 3:
	print("Usage: delete_all_documents.py project_id collection_id")
	sys.exit()

api_url=os.environ.get('WD_URL')
if api_url is None:
	print('WD_URL is not set. Set it to the Watson Discovery instance API URL.')
	sys.exit()
token=os.environ.get('WD_TOKEN')
if token is None:
	print('WD_TOKEN is not set. Set it to the Watson Discovery instance API TOKEN.')
	sys.exit()

project_id = sys.argv[1]
collection_id = sys.argv[2]

# Prepare the Authenticator. We will use the Bearer token
authenticator = BearerTokenAuthenticator( token )

discovery = DiscoveryV2(
    version='2021-04-20',
    authenticator=authenticator
)

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

discovery.set_disable_ssl_verification(True)
discovery.set_service_url( api_url )

# How many results for each query. Keep this <= 1000
batch_size=1000

# How many concurrent threads to use when sending delete requests.
max_threads=16

max_tries = 5

keep_deleting = True

# This taks will send the delete request to WD. This will be run from 
# a thread pool within the main loop.
def delete_task(did):
	#return "Will delete " + project_id + ", " + collection_id + ", " + did
	delete_response = discovery.delete_document(
		project_id=project_id, 
		collection_id=collection_id, 
		document_id=did
	).get_result()
	return delete_response
# End of delete_task


## Main loop
already_deleted = set([])

while keep_deleting:
	# Query the index and get the document_id (up to "batch_size" document_ids)
	for how_many_tries in range(max_tries):
		try:
			matched_documents = discovery.query(
				project_id=project_id, 
				collection_ids=[ collection_id ], 
				query='', 
				count=batch_size,
				return_=[ 'document_id' ]).get_result()
			#print (matched_documents)
			break
		except:
			print("Got an exception while querying the collection. Will try again...")
			time.sleep(1)
			
	if how_many_tries == max_tries - 1:
		print("Giving up.")
		sys.exit()

	# If there are still docunents in the index
	if matched_documents['matching_results'] > 0:
		results = matched_documents['results']
		docids = [ r['document_id'] for r in results ]
		#print(docids)

		# Compute the difference between the current set of document ids and those we already deleted.
		diff=set(docids).difference(already_deleted)
		#print (diff)

		if len(diff) == 0:
			# If there is no difference between the sets, then there is nothing for us to 
			# do yet. We have to wait for the delete requests to reach the index.
			# We will wait here for 30 seconds.
			print("Waiting for delete requests to reach the index...")
			time.sleep(30)
		else:
			# There are some documents we can delete. Loop over them and send the delete requests
			# using a thread pool.
			with ThreadPoolExecutor(max_workers = max_threads) as executor:
				# Runs the deletes
				delete_results = executor.map(delete_task, diff)
				# Report the delete results
				already_deleted = already_deleted.union(diff)
				for dr in delete_results:
					print(dr)

			print("{} delete requests sent.".format(len(already_deleted)))
	else:
		# No more documents in the index. Finish.
		print("No documents found in the collection.")
		keep_deleting = False

