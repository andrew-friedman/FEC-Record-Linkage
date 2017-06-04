import geocoder
import requests

def give_address(street, city, state, zip_code):
    if street == "Read_Error" or street == "Multiple_Error":
        full_address = city + ", " + state + " " + zip_code
    else:
        full_address =  street + ", " + city +", " + state + " " + zip_code
    g = geocoder.google(full_address)
    lat_long = g.latlng
    if not lat_long:
        full_address = city + " " + zip_code
        g = geocoder.google(full_address)
        lat_long = g.latlng
    return lat_long

def complile_url(lat_long):
    part_one_url = "http://boundaries.tribapps.com/1.0/boundary/?contains="
    part_two_url = str(lat_long[0]) + "," +str(lat_long[1])
    part_three_url =  "&sets=community-areas"
    full_url = part_one_url + part_two_url + part_three_url
    return full_url



def get_neighborhood(street, city, state, zip_code):
    lat_long = give_address(street, city, state, zip_code)
    url = complile_url(lat_long)
    request = requests.get(url)
    request_dict = request.json()
    objects = request_dict["objects"]
    if objects:
        neighborhood = objects[0]["name"]
    elif not objects:
        neighborhood = None
    return neighborhood


