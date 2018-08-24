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


auto load(const std::vector<std::vector<std::string> > &filenames_by_day,
          const std::vector<std::string> &output_files,
          const std::vector<std::string> &blacklist_vector,
          const bool only_meta = false) {
    using namespace pos_parser;
    unordered_set<string> blacklist(blacklist_vector.begin(), blacklist_vector.end());
    unordered_map<pos_parser::EAN , product_metadata> prod_meta;
    int day_count = 0;
    profit_map profit_all;
    profit_map margin_all;
    string_map sbu;
    unordered_set<pos_parser::household_id > b_house;
    unordered_set<pos_parser::person_id> b_person;
    for (const auto &filenames: filenames_by_day) {
        auto aggregated_transactions = process_day(filenames, blacklist, prod_meta, profit_all, margin_all,
                                                         sbu, b_house, b_person, only_meta);
        if (!only_meta) {
            std::cout<<time_str<<"Writing to file "<<output_files[day_count]<<std::endl;
            std::ofstream outstream(output_files[day_count++]);
            std::copy(std::istreambuf_iterator<char>(aggregated_transactions),
                      std::istreambuf_iterator<char>(),
                      std::ostreambuf_iterator<char>(outstream));
            //outstream << aggregated_transactions;
        }
    }
    return std::make_tuple(prod_meta, profit_all, margin_all, sbu, b_house, b_person);
}

namespace py = pybind11;

PYBIND11_MAKE_OPAQUE(std::vector<std::string>);
PYBIND11_MAKE_OPAQUE(std::vector<int>);
PYBIND11_MAKE_OPAQUE(std::vector<float>);
PYBIND11_MAKE_OPAQUE(pos_parser::string_map);
PYBIND11_MAKE_OPAQUE(pos_parser::metadata_map);
PYBIND11_MAKE_OPAQUE(pos_parser::profit_map);

PYBIND11_MODULE(_pos_parser, m)
{
    using namespace pos_parser;
    py::bind_vector<std::vector<std::string> >(m, "StrVector");
    py::bind_map<pos_parser::string_map>(m, "string_map");
    py::bind_vector<pos_parser::profit_map>(m, "profit_map");
    py::bind_map<pos_parser::metadata_map>(m, "metadat_map");
    m.def("process_day", process_day, "Process files for one day",
          py::return_value_policy::reference);

    m.def("load", load, "pos_parser main function",
          py::return_value_policy::reference);

    py::class_<pos_parser::product_metadata>(m, "product_metadata")
            .def_readonly("product_name", &product_metadata::product_name)
            .def_readonly("producthierarchylevel1", &product_metadata::producthierarchylevel1)
            .def_readonly("producthierarchylevel2", &product_metadata::producthierarchylevel2)
            .def_readonly("producthierarchylevel3", &product_metadata::producthierarchylevel3)
            .def_readonly("producthierarchylevel4", &product_metadata::producthierarchylevel4)
            .def_readonly("producthierarchylevel5", &product_metadata::producthierarchylevel5)
            .def_readonly("producthierarchylevel1name", &product_metadata::producthierarchylevel1name)
            .def_readonly("producthierarchylevel2name", &product_metadata::producthierarchylevel2name)
            .def_readonly("producthierarchylevel3name", &product_metadata::producthierarchylevel3name)
            .def_readonly("producthierarchylevel4name", &product_metadata::producthierarchylevel4name)
            .def_readonly("producthierarchylevel5name", &product_metadata::producthierarchylevel5name)
            .def_readonly("food_nonfood_ind", &product_metadata::food_nonfood_ind);

    py::class_<pos_parser::transaction_data>(m, "transaction_data")
            .def(py::init<const household_id &, const person_id &, const EAN &,
                    const double, const double, const string & >())
            .def_readwrite("household", &transaction_data::household)
            .def_readwrite("person", &transaction_data::person)
            .def_readwrite("ean", &transaction_data::ean)
            .def_readwrite("sales_price", &transaction_data::sales_price)
            .def_readwrite("sales_quantity", &transaction_data::sales_quantity)
            .def_readwrite("receipt_date", &transaction_data::receipt_date);
}

