import math

def pretty_print(x):
    units = ["", "K", "M", "G", "T"]
    unit = 0 
    while x > 1000:
        x = x / 1000
        unit += 1
    return str(x) + units[unit]


def compute_tree_structure(elements, fanout, cascading, force_uint64 = False):
    print(f"===== fanout {fanout} cascading {cascading} =====")
    tree_height = math.ceil(math.log(elements, fanout))
    print("height: ", tree_height)
    pure_tree_size = tree_height * elements
    print("pure tree size: ", pretty_print(pure_tree_size))
    pointer_annotations_cnt = (tree_height - 1) * elements * fanout /cascading
    print("pointer_annotations size: ", pretty_print(pointer_annotations_cnt))
    overall_cnt = pure_tree_size + pointer_annotations_cnt
    print("overall element count: ", pretty_print(overall_cnt))
    element_size = 8 if elements > pow(2, 32) or force_uint64 else 4
    print("element size: ", element_size)
    print("overall memory: ", pretty_print(overall_cnt * element_size))

size = 100*1000*1000
compute_tree_structure(size, 16, 4)
compute_tree_structure(size, 32, 32)

# The following results were double-checked against the actual results from `scalability_bench` to
# make sure the above equation is correct.
size = 1*1000*1000
compute_tree_structure(size, 16, 4, True)
compute_tree_structure(size, 32, 32, True)
compute_tree_structure(size, 512, 1, True)
compute_tree_structure(size, 1024, 2, True)
