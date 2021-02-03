import json
import argparse

def get_ids(business_json, categories):
	ids = set()
	with open(business_json,"r") as f:
		for line in f.readlines():
			json_record = json.loads(line)
			if json_record is None or len(json_record)==0 or json_record["categories"]==None:
				continue
			for category in json_record["categories"].split(","):
				if category.strip() in ["Restaurants"]:
					ids.add(json_record["business_id"])
					break
	return ids

def get_reviews(reviews_json, ids, output_json):
	with open(output_json, "w") as fw:
		selected_reviews_counter = 0
		counter=0
		with open(reviews_json,"r") as fr:
			for line in fr.readlines():
				if counter%1000==0:
					print("{}/{}".format(selected_reviews_counter,counter))
				counter +=1
				json_record = json.loads(line)
				if json_record["business_id"] in ids:
					selected_reviews_counter +=1
					json.dump(json_record, fw)
					fw.write("\n")
	print("Wrote {} records to '{}'".format(selected_reviews_counter,output_json))

def main(args):
	ids = get_ids(args.business, categories=['Restaurant'])
	print(len(ids))
	get_reviews(args.reviews, ids, args.output)




if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Extract reviews with category 'Restaurant' from yelp reviews")
	parser.add_argument('-r','--reviews',help='json of yelp reviews')
	parser.add_argument('-b','--business', help='json of yelp businesses')
	parser.add_argument('-o','--output', help='json of yelp restaurant reviews')
	args = parser.parse_args()
	main(args)