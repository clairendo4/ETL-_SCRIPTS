def read_properties(file_name):
    params_map = {}
    with open(file_name, 'r') as f:
        list_params = f.readlines()
        f.close()
    for each in list_params:
        each = each.replace("\n", "")
        params_map[each.split("=")[0]] = each.split("=")[1]
    return params_map
