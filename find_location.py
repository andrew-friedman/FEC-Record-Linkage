import geocoder
import requests

def give_address(street, city, state, zip_code):
    '''
    Find Latitude and longitude from Address
    
    Inputs:
      street: a string
      city: a string
      state: a string
      zip_code: a string
    
    Returns: a tuple with two floats
    '''
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
    '''
    Compiles the url for Chicago Tribunes' API for location
        from Latitude and longitude
    Inputs:
      lat_long: a tuple with two floats, Latitude and longitude
      
    Returns: a string, a url for Tribunes API
    '''
    part_one_url = "http://boundaries.tribapps.com/1.0/boundary/?contains="
    part_two_url = str(lat_long[0]) + "," +str(lat_long[1])
    part_three_url =  "&sets=community-areas"
    full_url = part_one_url + part_two_url + part_three_url
    return full_url



def get_neighborhood(street, city, state, zip_code):
    '''
    Gets the neighborhood from dictionary produced by the Chicago tribunes API
    
    Inputs:
      street: a string
      city: a string
      state: a string
      zip_code: a string      
      
    Return: None, if not in chicago or a string, Chicago neighborhood
    '''
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


