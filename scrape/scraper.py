import requests
import json
import pandas as pd
import pathlib
import os

import http.client

http.client._MAXHEADERS = 500000

try:
    FILE = pathlib.Path(__file__)
except NameError:
    FILE = pathlib.Path('.')

BASE = FILE.parent

proxy_pool = os.environ.get('PROXY_POOL', '127.0.0.1')
print("proxy_pool", proxy_pool)

def get_proxy():
    return requests.get("http://{}:5010/get/".format(proxy_pool)).json()


def delete_proxy(proxy):
    requests.get("http://{}:5010/delete/?proxy={}".format(proxy_pool, proxy))


def get_details(product_id):
    url = "https://www.walmart.com.mx/api/rest/model/atg/commerce/catalog/ProductCatalogActor/getProduct?id={0}".format(product_id)
    # headers = {
    # 'User-Agent': 'Mozilla/5.0 (compatible; MyCoolBot/1.0; +https://mycoolbot.com)',
    # }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    }
    proxy = requests.get("http://{}:5010/get/".format(proxy_pool)).json()
    response = requests.get(url, headers=headers, proxies={'http': proxy})
    data = response.json()
    return data, proxy


def add_zero_prefix(s):
    """Add a zero prefix to a string until it is 14 characters long."""
    s = str(s)
    while len(s) < 14:
        s = '0' + s
    return s


def create_dataframe_from_product_id(product_id):  
    proxy = None
    product_ids = []
    longDescriptions = []
    displayNames = []
    seoKeywords_list = []
    seoDescriptions = []
    metaDescriptions = []
    familyIds = []
    departmentNames = []
    familyNames = []
    departmentIds = []
    fineLineNames = []
    fineLineIds = []
    productSeoUrls = []
    skuPrices = []
    brands = []
    vendorNames = []
    sellerNames = []
    shippingCountrys = []
    smallImageUrls = []
    largeImageUrls = []
    descriptions=[]
    negotiations_list=[]
    maxQuantityAllowed_list=[]
    maxMSIs=[]
    isBulkBundles=[]
    metaTitles=[]
    repositoryIds=[]
    isGiftListProducts=[]
    isPreOrderables=[]
    itemHeights=[]
    itemWidths=[]
    itemWeights=[]
    itemLengths=[]
    returnableStatus_list=[]
    Ids=[]
    isBigItems=[]
    skuStatus_list=[]
    upcs=[]
    secondaryImages_list=[]
    sellingAtStores=[]
    formatIds=[]
    parentUpcs=[]
    offerIds=[]
    offer_actives=[]
    refurbished_list=[]

    product_ids.append(product_id)
    try:
        all_data, proxy = get_details(product_id)
    except:
        proxy and delete_proxy(proxy)
        all_data, proxy = get_details(product_id)        
    try:
        product = all_data['product']
    except:
        url = "https://www.walmart.com.mx/api/rest/model/atg/commerce/catalog/ProductCatalogActor/getProduct?id={0}".format(product_id)
        print(url)
    try:
        longDescription = product['longDescription']
    except:
        longDescription = None
    longDescriptions.append(longDescription)
    displayName = product['displayName']
    displayNames.append(displayName)
    description = product['description']
    descriptions.append(description)
    negotiations = product['negotiations']
    negotiations_list.append(negotiations)
    seoKeywords = product['seoKeywords']
    seoKeywords_list.append(seoKeywords)
    metaDescription = product['metaDescription']
    metaDescriptions.append(metaDescription)
    maxQuantityAllowed = product['maxQuantityAllowed']
    maxQuantityAllowed_list.append(maxQuantityAllowed)
    maxMSI = product['maxMSI']
    maxMSIs.append(maxMSI)
    isBulkBundle = product['isBulkBundle']
    isBulkBundles.append(isBulkBundle)
    familyId = product['breadcrumb']['familyId']
    familyIds.append(familyId)
    departmentName = product['breadcrumb']['departmentName']
    departmentNames.append(departmentName)
    familyName = product['breadcrumb']['familyName']
    familyNames.append(familyName)
    departmentId = product['breadcrumb']['departmentId']
    departmentIds.append(departmentId)
    try:
        fineLineName = product['breadcrumb']['fineLineName']
    except:
        fineLineName = None
    fineLineNames.append(fineLineName)
    try:
        fineLineId = product['breadcrumb']['fineLineId']
    except:
        fineLineId = None
    fineLineIds.append(fineLineId)
    metaTitle = product['metaTitle']
    metaTitles.append(metaTitle)
    repositoryId = product['repositoryId']
    repositoryIds.append(repositoryId)
    productSeoUrl = product['productSeoUrl']
    productSeoUrls.append(productSeoUrl)
    # productRatings = df.loc[index, 'productRatings']
    # freeShippingItem = df.loc[index, 'freeShippingItem']
    # skuPrice = df.loc[index, 'skuPrice']
    # skuPrices.append(skuPrice)
    brand = product['brand']
    brands.append(brand)
    isGiftListProduct = product['isGiftListProduct']
    isGiftListProducts.append(isGiftListProduct)
    # isGiftListProduct = product['childSKUs']
    isPreOrderable = product['childSKUs'][0]['isPreOrderable']
    isPreOrderables.append(isPreOrderable)
    itemHeight = product['childSKUs'][0]['itemHeight']
    itemHeights.append(itemHeight)
    itemWidth = product['childSKUs'][0]['itemWidth']
    itemWidths.append(itemWidth)
    itemWeight = product['childSKUs'][0]['itemWeight']
    itemWeights.append(itemWeight)
    itemLength = product['childSKUs'][0]['itemLength']
    itemLengths.append(itemLength)
    returnableStatus = product['childSKUs'][0]['returnableStatus']
    returnableStatus_list.append(returnableStatus)
    Id = product['childSKUs'][0]['id']
    Ids.append(Id)     
    isBigItem = product['childSKUs'][0]['isBigItem']
    isBigItems.append(isBigItem)
    seoDescription = product['childSKUs'][0]['seoDescription']
    seoDescriptions.append(seoDescription)
    skuStatus = product['childSKUs'][0]['skuStatus']
    skuStatus_list.append(skuStatus)
    upc = product['childSKUs'][0]['upc']
    upcs.append(upc)
    try:
        vendorName = product['childSKUs'][0]['vendorName']
    except:
        vendorName = None
    vendorNames.append(vendorName)
    try:
        secondaryImages = product['childSKUs'][0]['secondaryImages']
    except:
        secondaryImages = None
    secondaryImages_list.append(secondaryImages)
    sellingAtStore = product['childSKUs'][0]['sellingAtStore']
    sellingAtStores.append(sellingAtStore)
    formatId = product['childSKUs'][0]['formatId']
    formatIds.append(formatId)
    parentUpc = product['childSKUs'][0]['parentUpc']
    parentUpcs.append(parentUpc)
    try:
        offerId = product['childSKUs'][0]['offerList'][0]['offerId']
        offer_active = product['childSKUs'][0]['offerList'][0]['active']
        originalPrice = product['childSKUs'][0]['offerList'][0]['priceInfo']['originalPrice']
        refurbished = product['childSKUs'][0]['offerList'][0]['refurbished']
        sellerName = product['childSKUs'][0]['offerList'][0]['sellerName']
        shippingCountry = product['childSKUs'][0]['offerList'][0]['shippingCountry']
    except:
        offerId = None
        offer_active = None
        originalPrice = None
        refurbished = None
        sellerName = None
        shippingCountry = None
    offerIds.append(offerId)
    offer_actives.append(offer_active)
    refurbished_list.append(refurbished)
    skuPrices.append(originalPrice)
    sellerNames.append(sellerName)
    shippingCountrys.append(shippingCountry)
    smallImageUrl = product['childSKUs'][0]['smallImageUrl']
    largeImageUrl = product['childSKUs'][0]['largeImageUrl']
    smallImageUrls.append(smallImageUrl)
    largeImageUrls.append(largeImageUrl)
    
    dataframe = pd.DataFrame()
    dataframe['product_id'] = product_ids
    dataframe['longDescription'] = longDescriptions
    dataframe['displayName'] = displayNames
    dataframe['description'] = descriptions
    dataframe['negotiations'] = negotiations_list
    dataframe['seoKeywords'] = seoKeywords_list
    dataframe['seoDescription'] = seoDescriptions
    dataframe['metaDescription'] = metaDescriptions
    dataframe['maxQuantityAllowed'] = maxQuantityAllowed_list
    dataframe['maxMSI'] = maxMSIs
    dataframe['isBulkBundle'] = isBulkBundles
    dataframe['departmentName'] = departmentNames
    dataframe['familyName'] = familyNames
    dataframe['departmentId'] = departmentIds
    dataframe['fineLineName'] = fineLineNames
    dataframe['fineLineId'] = fineLineIds
    dataframe['metaTitle'] = metaTitles
    dataframe['repositoryId'] = repositoryIds
    dataframe['productSeoUrl'] = productSeoUrls
    dataframe['skuPrice'] = skuPrices
    dataframe['brand'] = brands
    dataframe['isGiftListProduct'] = isGiftListProducts
    dataframe['isPreOrderable'] = isPreOrderables
    dataframe['itemHeight'] = itemHeights
    dataframe['itemWidth'] = itemWidths
    dataframe['itemWeight'] = itemWeights
    dataframe['itemLength'] = itemLengths
    dataframe['returnableStatus'] = returnableStatus_list
    dataframe['Id'] = Ids
    dataframe['isBigItem'] = isBigItems
    dataframe['skuStatus'] = skuStatus_list
    dataframe['upc'] = upcs
    dataframe['vendorName'] = vendorNames
    dataframe['secondaryImages'] = secondaryImages_list
    dataframe['sellingAtStore'] = sellingAtStores
    dataframe['formatId'] = formatIds
    dataframe['parentUpc'] = parentUpcs
    dataframe['offerId'] = offerIds
    dataframe['offer_active'] = offer_actives
    dataframe['refurbished'] = refurbished_list
    dataframe['sellerName'] = sellerNames
    dataframe['shippingCountry'] = shippingCountrys
    dataframe['smallImageUrl'] = smallImageUrls
    dataframe['largeImageUrl'] = largeImageUrls
    return dataframe   

dataset_path = BASE / 'dataset.csv'
dataset_path = dataset_path.as_posix()

df = pd.read_csv(dataset_path, index_col=0)

# TODO: Write logic to load saved files

df['productId'] = df['productId'].apply(lambda x: add_zero_prefix(x))
product_ids = df['productId'].tolist()

dataf = pd.DataFrame()
visited_product_ids = []
enum = 0


for product_id in product_ids[len(visited_product_ids):]:
    enum += 1
    try:
        dff = create_dataframe_from_product_id(product_id)
    except:
        try:
            dff = create_dataframe_from_product_id(product_id)
        except Exception as e:
            url = "https://www.walmart.com.mx/api/rest/model/atg/commerce/catalog/ProductCatalogActor/getProduct?id={0}".format(product_id)
            print("url1:", url)
            url = 'https://www.walmart.com.mx' + df[df['productId'] == str(product_id)]['productSeoUrl'].tolist()[0]
            print("url2:", url)
            print(e)
            break
    dataf = dataf.append(dff)
    visited_product_ids.append(product_id)
    if enum % 10000==0:
        intermediate_dataset_path = BASE / 'dataset_{}.csv'.format(enum)
        intermediate_dataset_path = intermediate_dataset_path.as_posix()
        dataf.to_csv(intermediate_dataset_path)
        print('dataset_{}.csv saved successfully'.format(enum))
    print("{} completed".format(enum))


final_dataset_path = BASE / 'complete_dataset.csv'
final_dataset_path = final_dataset_path.as_posix()
dataf.to_csv(final_dataset_path)

