import json
import boto3
import decimal
import sys
import urllib.parse
from urllib.request import urlopen, Request


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class DynamoDB():
    """ This will be the class that we will use to access the DyanmDB table.
    It will contain all the functions that will be needed to read/write to
    the table, plus a few functions that will analize the table.

    Args: We will need the name of the table within the region that we have 
    choosen.

    TableAttributes: 
        Unique_ID
        ilvl
        item
        quantity
    """

    def __init__(self, table):
        """ Simple initialization for you DynamoDB client and table. This 
        will give use access to the right table that we are looking for.
        """
        self.table_id = table
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(self.table_id)

    # These functions are for accesssing information for the table with API
    # Gateway calls. Every function will return a JSON object with a: header,
    # Body and StatusCode.
    def find_top_quantity(self, number_of_items=20):
        """ This will find the top number_of_items from the Dynamodb table 
        passed through a API call. This will only find the 'Top' items 
        if there is a 'ilvl', and a 'quantity' attribute within the table.
        This table will default number_of_items = 20 unless called with a 
        different amount.

        Args: A Dynamodb table.

        Returns: A JSON object with the top number_of_items in it. It will be
        of the form:
        [
            {
            "item": item,
            "ilvl": ilvl,
            "quantity": quantity
            }
        ]
        """

        # This will be used to get all the items from the table and then
        lvl_dict = {}
        response = self.table.scan()
        for item in response['Items']:
            lvl_dict[item['Unique_ID']] = {
                'item': item['item'],
                'ilvl': item['ilvl'],
                'quantity': item['quantity']
            }

        while 'LastEvaluatedKey' in response:
            last_key = response['LastEvaluatedKey']
            response = self.table.scan(
                ExclusiveStartKey=last_key,
            )
            for item in response['Items']:
                lvl_dict[item['Unique_ID']] = {
                    'item': item['item'],
                    'ilvl': item['ilvl'],
                    'quantity': item['quantity']
                }

        # We can simply return a statusCode of 500 if the table is empty.
        if len(lvl_dict) == 0:
            response = {
                "message": "Table is empty , please wait for maintanence"
            }
            return response

        # These are the variables and tables needed to keep track of the
        # iterations. We also needed to order the items into a resverse order
        # based off the item lvl.
        sorted_lvl_dict = {k: v for k, v in sorted(
            lvl_dict.items(), key=lambda item: item[0]
        )
        }
        counter = 0
        total = 0
        json_list = []

        # Once we have the sorted list we can then count the quantity and
        # the get the items untill we reach the correct amount of items.
        for item in sorted_lvl_dict:
            if sorted_lvl_dict[item]['quantity'] > 0:
                total += int(sorted_lvl_dict[item]['quantity'])
                counter += 1
                json_list.append(sorted_lvl_dict[item])
                print("name: {},\t\t ilvl: {},\t quantity: {}".format(
                    sorted_lvl_dict[item]['item'],
                    sorted_lvl_dict[item]['ilvl'],
                    sorted_lvl_dict[item]['quantity']
                )
                )
            if total >= number_of_items or counter >= number_of_items:
                break
        return json_list

    def put_item(self, item, ilvl, quantity):
        """This will take a client for a DynamoDB table, an item, and the 
        ilvl of that item. It will create the Unique_ID for the item and 
        PUT the item into the table. 

        Args: This will take a client for a DynamoDB table, an item, and 
        the ilvl of that item.

        Returns: The response from the DynamoDB table.
        """
        item = url_decode(item)
        Unique_ID = create_unique_ilvl_str(ilvl, item)
        response = self.table.update_item(
            Key={
                'Unique_ID': Unique_ID,
            },
            UpdateExpression="SET quantity = :q, ilvl = :l, #it = :i",
            ExpressionAttributeValues={
                ":q": decimal.Decimal(quantity),
                ":l": ilvl,
                ":i": item
            },
            ExpressionAttributeNames={
                "#it": "item"
            },
            ReturnValues="ALL_NEW"
        )
        response = {
            "message": "Item has been added to the table.",
            "Item": [Unique_ID, item, ilvl, quantity]
        }
        return response

    def get_item(self, item, ilvl):
        """ This is used to get an item from the DynamoDB table with the class.
        You need to pass the item and the ilvl of the item. You can pass the 
        resource directly to this function and it will decode the resource
        automatically.

        Args: The item and the ilvl of the item.

        Returns: The response from the DynamoDB table.
        """
        item = url_decode(item)
        ID = create_unique_ilvl_str(ilvl, item)
        response = self.table.get_item(
            Key={
                'Unique_ID': ID
            }
        )
        if "Item" in response:
            response_item = response['Item']
            return response_item
        else:
            response = {
                "message": "Item not in table please try an other.",
                "item": [item, ilvl]
            }
            return response

    def delete_item(self, item, ilvl):
        """ This will clear an item from the Dynamodb table passed within the 
        class.

        Args: The item and it's ilvl

        Returns: A JSON object with: StatusCode Body and Header. Keep in mind
        that delete_item will never return a error even if the item isn't in the
        table when the call is made.
        """
        item = url_decode(item)
        ilvl = ilvl
        unique = create_unique_ilvl_str(ilvl, item)

        response = self.table.delete_item(
            Key={
                'Unique_ID': unique
            },
            ReturnValues="ALL_OLD"
        )
        response = {
            "message": "Item has been deleted from the table.",
            "Item": [unique, item, ilvl]
        }
        return response

    # These are functions to augment the table, but these are not called from an API
    # These do not need to return anything as all the logging is done with
    # print().
    def update_table(self, unique_items):
        """ This will be used to update the quantity for any item passed through the
        function call in the Dict Unique_items. This will only process one tab at a 
        time.

        Args: The client for the DynamoDB table that you want updated and the Dict 
        with Quantities that you want updated.

        Returns: Null
        """
        counter = 0
        for item in unique_items:
            counter += 1
            get_response = self.table.get_item(
                Key={
                    'Unique_ID': item
                }
            )

            if 'Item' in get_response:
                response = self.table.update_item(
                    Key={
                        'Unique_ID': item
                    },
                    UpdateExpression="SET quantity = quantity + :q",
                    ExpressionAttributeValues={
                        ":q": decimal.Decimal(unique_items[item]['quantity']),
                    },
                    ReturnValues="ALL_NEW"
                )
            else:
                response = self.table.update_item(
                    Key={
                        'Unique_ID': item
                    },
                    UpdateExpression="SET quantity = :q",
                    ExpressionAttributeValues={
                        ":q": decimal.Decimal(unique_items[item]['quantity']),
                    },
                    ReturnValues="ALL_NEW"
                )

            print("Item: {}, ilvl: {}, Quantity: {}.".format(
                response['Attributes']['item'],
                response['Attributes']['ilvl'],
                response['Attributes']['quantity']
            )
            )
        print('Total number of writes to the table {}.'.format(counter))

    def upload_stash(self, current_stash):
        """ This will be used to upload all items in the current_stash to the 
        DynamoDB table stored within the class. This will only process the 
        current_stash.

        Args: The current_stash from the PoE public API.

        Returns: Null
        """
        counter = 0
        counter_unique = 0
        for stash in current_stash['stashes']:
            for row in stash['items']:
                counter += 1
                if row['name'] != '' and row['ilvl'] != '':
                    unique_ = create_unique_ilvl_str(row['ilvl'], row['name'])
                    get_response = self.table.get_item(
                        Key={
                            'Unique_ID': unique_
                        }
                    )

                    if 'Item' in get_response:
                        continue

                    else:
                        counter_unique += 1
                        response = table.update_item(
                            Key={
                                'Unique_ID': unique_,
                            },
                            UpdateExpression="SET quantity = :q, ilvl = :l, #it = :i",
                            ExpressionAttributeValues={
                                ":q": int(0),
                                ":l": row['ilvl'],
                                ":i": row['name']
                            },
                            ExpressionAttributeNames={
                                "#it": "item"
                            },
                            ReturnValues="ALL_NEW"
                        )
                        print("name: {},\t\t ilvl: {},\t quantity: {},\t unique: {}".format(
                            response['Attributes']['item'],
                            response['Attributes']['ilvl'],
                            response['Attributes']['quantity'],
                            response['Attributes']['Unique_ID']
                        )
                        )
                if counter % 100 == 0:
                    print("{} have been processed of {} total items.".format(
                        counter_unique, counter
                    )
                    )

    def reset_table(self):
        """ This will set every quantity value to 0. This is best run from a home
        console as this will take longer than the 15min time out that Lambda 
        functions have.

        Args: A Dynamodb table.

        Returns: Null
        """
        # We need to find all the unique IDs
        unique_ids = []
        counter = 0

        response = self.table.scan(
            FilterExpression="quantity > :z",
            ExpressionAttributeValues={
                ":z": decimal.Decimal(0)
            }
        )
        for unique in response['Items']:
            unique_ids.append(unique['Unique_ID'])

        while 'LastEvaluatedKey' in response:
            last_key = response['LastEvaluatedKey']
            response = self.table.scan(
                ExclusiveStartKey=last_key,
                FilterExpression="quantity > :z",
                ExpressionAttributeValues={
                    ":z": decimal.Decimal(0)
                }
            )

            for unique in response['Items']:
                unique_ids.append(unique['Unique_ID'])

        print('{} Unique_IDs found.'.format(len(unique_ids)))

        # Now we can update the quantity column
        for unique in unique_ids:
            counter += 1
            response = self.table.update_item(
                Key={
                    'Unique_ID': unique
                },
                UpdateExpression="SET quantity = :q",
                ExpressionAttributeValues={
                    ":q": decimal.Decimal(0),
                },
                ReturnValues="ALL_OLD"
            )

            print("name: {},\t\t ilvl: {},\t quantity: {},\t unique: {}".format(
                response['Attributes']['item'],
                response['Attributes']['ilvl'],
                response['Attributes']['quantity'],
                response['Attributes']['Unique_ID']
            )
            )
            if counter % 100 == 0:
                print('{} items reset.'.format(counter))
                print('{} items left.'.format(str(len(unique_ids) - counter)))

    def upload_top_items(self, item_list):
        """ This will be for the smaller PoE_top_items table. This is to avoid 
        the long time it takes to get the top items from the DynamoDB table
        as taking the top items requires scanning the entire table. The step 
        prior to running this function should be to delete every item in the
        table.

        Args: The list of items that we want to upload to the smaller DynamoDB
        table.

        Returns: Null.
        """
        for item in item_list:
            item_ = item["item"]
            ilvl_ = item["ilvl"]
            quantity_ = item["quantity"]
            print(
                "Item: {}, Ilvl: {}, Quantity: {}".format(
                    item_, ilvl_, quantity_)
            )
            self.put_item(item_, ilvl_, quantity_)

    def delete_items(self):
        """ This will clear the enitire database for the DynamoDB table that 
        the class connects to. Once this starts it will start to clear items 
        from the DynamoDB table.

        Args: Null

        Returns: Null
        """
        items = []
        counter = 0
        response = self.table.scan()
        for item in response['Items']:
            items.append(item['Unique_ID'])
        while 'LastEvaluatedKey' in response:
            last_key = response['LastEvaluatedKey']
            response = self.table.scan(
                ExclusiveStartKey=last_key,
            )
            for item in response['Items']:
                items.append(item['Unique_ID'])

        number_of_items_remove = len(items)
        print('removing {} items'.format(number_of_items_remove))
        for Unique_ID in items:
            counter += 1
            response = self.table.delete_item(
                Key={
                    'Unique_ID': Unique_ID
                },
                ReturnValues="ALL_OLD"
            )
            if counter % 100 == 0:
                print('{} items left'.format(number_of_items_remove - counter))


# These are the helper functions for the all the classes called here. These are
# not part of the DynamoDB class, so any script can use them without creating
# a DynamoDB object.
def create_unique_ilvl_str(ilvl, item):
    """ This is used to create the string for the item level but in reverse 
    order for the key ID.

    Args: A object that can be interpreted as a int, and an object that can 
    be interpreted as 
        a string.

    Returns: The string for the unique key ID.
    """
    if int(ilvl) <= 90:
        unique_ID = "0" + str(100 - int(ilvl)) + str(item)
    else:
        unique_ID = "00" + str(100 - int(ilvl)) + str(item)
    return unique_ID


def url_decode(string):
    """ This is used to decode the resources passed through the event. 
    We need to use this as any resource passed will not be able to use
    standard spaces and other common character.

    Args: You will need the url resource or even a entire URL.

    Returns: The decoded resourse or URL.
    """
    encoded_string = string
    return urllib.parse.unquote(encoded_string)


def load_url(url):
    """ This will take an input of a http and create a false header to get
        around the Forbiddin error and load the API.

        Args: A HTTP.

        Returns: An API.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.3'
    }
    reg_url = url
    req = Request(url=reg_url, headers=headers)
    html = urlopen(req).read()
    return html


def load_JSON(site):
    """ This will take an API and turn it into a JSON.

    Args: An API.

    Returns: A JSON object.
    """
    data = json.loads(site)
    return data


# These are helper functions specific to the PoE public API stash.
def get_PoE_stash():
    """ This is used to get the first of the public PoE tabs, every other 
    stash_id depends on this one.

    Args: Null

    Returns: The JSON object for the first PoE Tab.
    """
    stash_api = load_url(
        "https://www.pathofexile.com/api/public-stash-tabs?id=0"
    )
    return load_JSON(stash_api)


def get_next_stash(next_id):
    """ This is used to get the next tab of the public PoE tab, this requires 
    the next id that a previous call will generate.

    Args: The next id in string format

    Returns: The stash in JSON format.
    """
    stash_api = load_url(
        "https://www.pathofexile.com/api/public-stash-tabs?id={}".format(
            next_id)
    )
    return load_JSON(stash_api)


def get_next_id(object):
    """ This pulls the next change id out of the JSON object from the PoE 
    public api.

    Args: The next_change_id from the PoE API tab.

    Returns: The next change id needed for a other get.
    """
    return object['next_change_id']


def get_stash_quantities(item_list):
    """ This will be used to get the unique ids for the PoE API stash that is 
    passed as an argument for this function. This is used to avoid to much 
    read/write for a DynamoDB table. This will only process one tab at a time

    Args: A JSON object that contains the current PoE API Tab.

    Returns: A Dict that contains the Unique_ID (as the primary key), item, ilvl
    and quantity.
    """
    # Variables for the totals for prints.
    counter = 0
    counter_unique = 0
    unique_items = {}

    for stash in item_list['stashes']:
        for row in stash['items']:
            counter += 1

            if row['name'] != '' and row['ilvl'] != '':
                unique_ = create_unique_ilvl_str(row['ilvl'], row['name'])
                level_ = row['ilvl']
                item_ = row['name']

                if unique_ in unique_items:
                    unique_items[unique_]['quantity'] += 1
                else:
                    counter_unique += 1
                    unique_items[unique_] = {
                        "ilvl": level_,
                        "item": item_,
                        "quantity": decimal.Decimal(1)
                    }

            if counter % 100 == 0:
                print(
                    "{} have been added to Unique_items of {} total items.".format(
                        counter_unique, counter)
                )
    return unique_items
