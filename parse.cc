#include <vector>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <iterator>
#include <tuple>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include "pos_parser.h"

namespace py = pybind11;

int main() {
    py::object _py_pos_parse = py::module::import("pos_parser");
    py::object _get_paths_func = _py_pos_parse.attr("get_paginated_day_paths_from_source");
    py::list path_list = _get_paths_func();
    for (auto path_for_day: path_list) {
        for (auto &file: path_for_day)
        std::cout << std::string(py::reinterpret_steal<py::str>(file)) << std::endl;
    }
    return 0;
}
