import os
import os.path

format_dir = ["example", "include", "sdk", "service", "storage", "txindex", "txplanner"]
path = os.getcwd()

for p in format_dir:
    p = path + '/' + p
    for root, dirs, files in os.walk(p):
        for name in files:
            if (name.endswith(".h")) or (name.endswith(".cpp")):
                localpath = root + '/' + name
                print(localpath)
                os.system("clang-format -i %s" % (localpath))
