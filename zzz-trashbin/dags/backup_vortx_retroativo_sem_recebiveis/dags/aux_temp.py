from common import common

res = common.read_config(verbose=True)

path_vortx_api = res["PATH_VORTX_API"]

print(path_vortx_api)