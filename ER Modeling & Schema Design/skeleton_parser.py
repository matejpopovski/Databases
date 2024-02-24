import sys
from json import loads
from re import sub

MONTHS = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
          'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

columnSeparator = "|"


def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'


def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]


def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon


def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)


def parseJson(json_file):
    with open(json_file, 'r') as f:
        # creates a Python dictionary of Items for the supplied json file
        items = loads(f.read())['Items']
        file1 = open("category.dat", "a")
        file2 = open("user.dat", "a")
        file3 = open("bids.dat", 'a')
        file4 = open("items.dat", 'a')
        for item in items:
            """
            TODO: traverse the items dictionary to extract information from the
            given `json_file' and generate the necessary .dat files to generate
            the SQL tables based on your relation design
            """
            # category table
            categories = item["Category"]
            itemID = item["ItemID"]
            if itemID == None:
                itemID = "NULL"
            for category in categories:
                line = ['\"' + sub('\"', '\"\"', category) + '\"', itemID]
                file1.write("|".join(line) + "\n")

            # items table
            itemRow = []
            if not item["ItemID"] == None:
                itemRow.append(str(item["ItemID"]))
            else:
                itemRow.append("NULL")
            if not item["Seller"]["UserID"] == None:
                itemRow.append(
                    '\"' + sub('\"', '\"\"', str(item["Seller"]["UserID"])) + '\"')
            else:
                itemRow.append("NULL")
            if not item["Name"] == None:
                itemRow.append(
                    '\"' + sub('\"', '\"\"', str(item["Name"])) + '\"')
            else:
                itemRow.append("NULL")
            if not item["Number_of_Bids"] == None:
                itemRow.append(item["Number_of_Bids"])
            else:
                itemRow.append("NULL")
            if not item["First_Bid"] == None:
                itemRow.append(str(transformDollar(item["First_Bid"])))
            else:
                itemRow.append("NULL")
            if not "Buy_Price" in item.keys() or item["Buy_Price"] == None:
                itemRow.append("NULL")
            else:
                itemRow.append(str(transformDollar(item["Buy_Price"])))
            if not item["Currently"] == None:
                itemRow.append(str(transformDollar(item["Currently"])))
            else:
                itemRow.append("NULL")
            if not item["Started"]:
                itemRow.append(str(transformDttm(item["Started"])))
            else:
                itemRow.append("NULL")
            if not item["Ends"] == None:
                itemRow.append(str(transformDttm(item["Ends"])))
            else:
                itemRow.append("NULL")
            if not item["Description"] == None:
                itemRow.append(
                    '\"' + sub('\"', '\"\"', str(item["Description"])) + '\"')
            else:
                itemRow.append("NULL")
            strItemRow = "|".join(itemRow) + "\n"
            file4.write(strItemRow)

            # users table: sellers
            if not item["Seller"]["UserID"] == None:
                users = [
                    '\"' + sub('\"', '\"\"', item["Seller"]["UserID"]) + '\"']
            else:
                users = ["NULL"]
            if not item["Location"] == None:
                users.append('\"' + sub('\"', '\"\"', item["Location"]) + '\"')
            else:
                users.append("NULL")
            if not item["Country"] == None:
                users.append('\"' + sub('\"', '\"\"', item["Country"]) + '\"')
            else:
                users.append("NULL")
            if not item["Seller"]["Rating"] == None:
                users.append(item["Seller"]["Rating"])
            else:
                users.append("NULL")
            file2.write("|".join(users) + "\n")

            # users table: bidders
            if not item["Bids"] == None:
                for i in range(len(item["Bids"])):
                    bidder = item["Bids"][i]["Bid"]["Bidder"]
                    if not bidder["UserID"] == None:
                        line = [
                            '\"' + sub('\"', '\"\"', bidder["UserID"]) + '\"']
                    else:
                        line = ["NULL"]
                    if not "Location" in bidder.keys() or bidder["Location"] == None:
                        line.append("NULL")
                    else:
                        line.append('\"' + sub('\"', '\"\"',
                                    bidder["Location"]) + '\"')
                    if not "Country" in bidder.keys() or bidder["Country"] == None:
                        line.append("NULL")
                    else:
                        line.append('\"' + sub('\"', '\"\"',
                                    bidder["Country"]) + '\"')
                    if not bidder["Rating"] == None:
                        line.append(bidder["Rating"])
                    else:
                        line.append("NULL")
                    file2.write("|".join(line) + "\n")

            # bids table
            bids = item["Bids"]
            if item["ItemID"] == None:
                item_id = "NULL"
            else:
                item_id = item["ItemID"]
            if bids == None:
                # add the item id and have the rest of the attributes be null
                file3.write(item_id + "|" + "NULL" + "|" +
                            "NULL" + "|" + "NULL" + "\n")
                continue
            for bid in bids:
                if bid["Bid"]["Bidder"]["UserID"] == None:
                    bidder_id = "NULL"
                else:
                    bidder_id = '\"' + \
                        sub('\"', '\"\"', bid["Bid"]
                            ["Bidder"]["UserID"]) + '\"'
                if bid["Bid"]["Time"] == None:
                    bid_time = "NULL"
                else:
                    bid_time = transformDttm(bid["Bid"]["Time"])

                if bid["Bid"]["Amount"] == None:
                    bid_amount = "NULL"
                else:
                    bid_amount = transformDollar(bid["Bid"]["Amount"])
                file3.write(item_id + "|" + bidder_id + "|" +
                            bid_time + "|" + bid_amount + "\n")
        file1.close()
        file2.close()
        file3.close()
        file4.close()


for i in range(0, 40):
    f = ".\ebay_data\items-" + str(i) + ".json"
    parseJson(f)
    print("Success parsing " + f)
