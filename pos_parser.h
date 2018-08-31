#pragma once

#include <iostream>
#include <future>
#include <any>
#include <fstream>
#include <numeric>
#include <vector>
#include <iterator>
#include <iomanip>
#include <thread>
#include <string>
#include <cmath>
#include <cstring>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

std::ostream &time_str(std::ostream &out) {
    std::time_t t = std::time(nullptr);
    std::tm tme = *std::localtime(&t);
    out << std::put_time(&tme, "%F %T %Z\t");
    return out;
}

template<typename FutureT>
void wait_on_futures(const std::vector<FutureT> &futures) {
    bool complete = false;
    while (!complete) {
        for (auto &fut: futures) {
            complete = true;
            if (fut.wait_for(std::chrono::seconds(2)) != std::future_status::ready) {
                complete = false;
                break;
            }
        }
    }
}

namespace std {
    template<> struct hash<std::tuple<uint64_t, string> >
    {
        typedef std::tuple<uint64_t, string> argument_type;
        typedef std::size_t result_type;
        result_type operator()(argument_type const& s) const noexcept
        {
            result_type const h1 ( std::hash<unsigned long>{}(std::get<0>(s)) );
            result_type const h2 ( std::hash<std::string>{}(std::get<1>(s)) );
            return h1 ^ (h2 << 1);
        }
    };
}

namespace pos_parser {
    using std::vector;
    using std::string;
    using std::unordered_set;
    using std::unordered_map;
    typedef unordered_map<string, string> string_map;
    typedef uint64_t EAN;
    typedef string sbu_id;
    typedef uint64_t household_id;
    typedef uint64_t person_id;
    typedef vector<std::tuple<EAN, sbu_id, double> > profit_map;

    struct product_metadata {
        string product_name;
        string producthierarchylevel1;
        string producthierarchylevel2;
        string producthierarchylevel3;
        string producthierarchylevel4;
        string producthierarchylevel5;
        string producthierarchylevel1name;
        string producthierarchylevel2name;
        string producthierarchylevel3name;
        string producthierarchylevel4name;
        string producthierarchylevel5name;
        string food_nonfood_ind;
    };

    enum pos_columns {
        PLUSSA_CARDNUMBER = 0,
        CUSTCONTACT_ID = 1,
        PERSON_ID = 2,
        HOUSEHOLD_ID = 3,
        EAN_CODE = 4,
        PRODUCT_NAME = 5,
        PRODUCTHIERARCHYLEVEL1 = 6,
        PRODUCTHIERARCHYLEVEL1NAME = 7,
        PRODUCTHIERARCHYLEVEL2 = 8,
        PRODUCTHIERARCHYLEVEL2NAME = 9,
        PRODUCTHIERARCHYLEVEL3 = 10,
        PRODUCTHIERARCHYLEVEL3NAME = 11,
        PRODUCTHIERARCHYLEVEL4 = 12,
        PRODUCTHIERARCHYLEVEL4NAME = 13,
        PRODUCTHIERARCHYLEVEL5 = 14,
        PRODUCTHIERARCHYLEVEL5NAME = 15,
        SALES_PRICE = 16,
        VAT_AMOUNT = 17,
        SALES_QUANTITY = 18,
        RECEIPT_DATE = 19,
        SITE_ID = 20,
        SOURCEBUSINESSUNIT_ID = 21,
        CUSTCHAIN_ID = 22,
        CHAINUNIT_ID = 23,
        RECEIPT_ID = 24,
        FOOD_NONFOOD_IND = 25,
        CUST_TYPE_CD = 26,
        TRANSACTION_GROSS_AMT_EUR = 27,
        PROFIT_AMT_EUR = 28,
        LOSS_AMT_EUR = 29,
    };

    struct transaction_data {
        household_id household;
        person_id person;
        EAN ean;
        double sales_price;
        double sales_quantity;
        string receipt_date;
    };

    auto stof = [](const std::string &str) {
        return std::strtod(str.c_str(), (char **) nullptr);
    };

    auto stoul = [](const std::string &str) {
        return std::strtoul(str.c_str(), (char **) nullptr, 10);
    };

    typedef unordered_map<EAN, product_metadata> metadata_map;
    typedef std::tuple<vector<transaction_data>, profit_map, profit_map,
            string_map, metadata_map,
            unordered_set<household_id >, unordered_set<person_id> > return_type;

    return_type process_single_file(const string &filename,
                                    const unordered_set<string> &blacklisted_acct,
                                    bool only_meta) {
        const string PLUSSA_HEADER = "PlussaCardNumber";
        vector<string> fields;
        unordered_set<household_id> blacklisted_household;
        unordered_set<person_id> blacklisted_person;
        unordered_map<string, string> sbu_map;
        unordered_map<EAN, product_metadata> metadata_map;
        profit_map profit;
        profit_map margin;
        fields.reserve(31);
        std::ifstream fstr(filename);
        string line;
        string token;
        vector<transaction_data> output;
        output.reserve(400'000);
        profit.reserve(400'000);
        margin.reserve(400'000);
        std::cout<<time_str<<"Processing "<<filename<<std::endl;
        int line_count = 0;
        while (std::getline(fstr, line)) {
            fields.clear();
            if (0 == std::strncmp(PLUSSA_HEADER.c_str(), line.c_str(), PLUSSA_HEADER.size())) {
                continue;
            }
            std::istringstream lstr(line);
            while (std::getline(lstr, token, '|')) {
                fields.push_back(token);
            }
            if (stof(fields[SALES_PRICE]) < 0 || stof(fields[SALES_QUANTITY]) < 0) {
                continue;
            }
            if (!metadata_map.count(stoul(fields[EAN_CODE]))) {
                product_metadata metadata{
                        .product_name = fields[PRODUCT_NAME],
                        .producthierarchylevel1 = fields[PRODUCTHIERARCHYLEVEL1],
                        .producthierarchylevel2 = fields[PRODUCTHIERARCHYLEVEL2],
                        .producthierarchylevel3 = fields[PRODUCTHIERARCHYLEVEL3],
                        .producthierarchylevel4 = fields[PRODUCTHIERARCHYLEVEL4],
                        .producthierarchylevel5 = fields[PRODUCTHIERARCHYLEVEL5],
                        .producthierarchylevel1name = fields[PRODUCTHIERARCHYLEVEL1NAME],
                        .producthierarchylevel2name = fields[PRODUCTHIERARCHYLEVEL2NAME],
                        .producthierarchylevel3name = fields[PRODUCTHIERARCHYLEVEL3NAME],
                        .producthierarchylevel4name = fields[PRODUCTHIERARCHYLEVEL4NAME],
                        .producthierarchylevel5name = fields[PRODUCTHIERARCHYLEVEL5NAME],
                        .food_nonfood_ind = fields[FOOD_NONFOOD_IND]
                };
                metadata_map[stoul(fields[EAN_CODE])] = metadata;
            }

            if (!sbu_map.count(fields[SOURCEBUSINESSUNIT_ID])) {
                sbu_map[fields[SOURCEBUSINESSUNIT_ID]] = fields[SITE_ID];
            }

            const auto profit_amount =
                    (stof(fields[PROFIT_AMT_EUR]) - stof(fields[LOSS_AMT_EUR])) / stof(fields[SALES_QUANTITY]);
            const auto margin_amount = (stof(fields[PROFIT_AMT_EUR]) - stof(fields[LOSS_AMT_EUR])) /
                                       stof(fields[TRANSACTION_GROSS_AMT_EUR]);
            if (!std::isnan(profit_amount)) {
                profit.push_back(std::make_tuple(stoul(fields[EAN_CODE]), fields[SOURCEBUSINESSUNIT_ID], profit_amount));
            }
            if (!std::isnan(margin_amount)) {
                margin.push_back(std::make_tuple(stoul(fields[EAN_CODE]), fields[SOURCEBUSINESSUNIT_ID], margin_amount));
            }
            if (blacklisted_acct.count(fields[CUSTCONTACT_ID])
                && !blacklisted_household.count(stoul(fields[HOUSEHOLD_ID]))) {
                blacklisted_household.insert(stoul(fields[HOUSEHOLD_ID]));
                blacklisted_person.insert(stoul(fields[PERSON_ID]));
            }
            if (!only_meta) {
                output.push_back(transaction_data{
                        .household = stoul(fields[HOUSEHOLD_ID]),
                        .person = stoul(fields[PERSON_ID]),
                        .ean = stoul(fields[EAN_CODE]),
                        .sales_price = stof(fields[SALES_PRICE]),
                        .sales_quantity = stof(fields[SALES_QUANTITY]),
                        .receipt_date = fields[RECEIPT_DATE]});
            }
            line_count++;
        }
        std::remove(filename.c_str());
        std::cout<<time_str<<"\t"<<filename<<"\t"<<line_count<<std::endl;
        return std::make_tuple(output, profit, margin, sbu_map, metadata_map,
                               blacklisted_household, blacklisted_person);
    }


    auto process_day(const vector<string> &filenames,
                     const unordered_set<string> &blacklist,
                     metadata_map &prod_meta,
                     profit_map &profit_all,
                     profit_map &margin_all,
                     string_map &sbu,
                     unordered_set<household_id> &b_house,
                     unordered_set<person_id> &b_person,
                     bool only_meta) {
        vector<std::future<return_type>> futures;
        vector<std::future<std::any>> merge_futures;
        std::stringstream transaction_data;
        profit_map agg_profit;
        profit_map agg_margin;

        for (const auto &filename: filenames) {
            futures.push_back(std::async(std::launch::async, process_single_file, filename, blacklist, only_meta));
        }
        wait_on_futures(futures);
        for (auto &f: futures) {
            auto [trans, profit, margin, sbu_map, metadata, black_house, black_person] = f.get();
            for (const auto & transline: trans) {
                transaction_data << transline.person << '\t'
                                 << transline.household << '\t'
                                 << transline.ean << '\t'
                                 << transline.sales_price << '\t'
                                 << transline.sales_quantity << '\t'
                                 << transline.receipt_date << '\n';
            }
            std::thread t0([](profit_map &accum, profit_map &val) {
                accum.insert(accum.end(), std::make_move_iterator(val.begin()), std::make_move_iterator(val.end()));
                }, std::ref(profit_all), std::ref(profit));
            std::thread t1([](profit_map &accum, profit_map &val) {
                accum.insert(accum.end(), std::make_move_iterator(val.begin()), std::make_move_iterator(val.end()));
            }, std::ref(margin_all), std::ref(margin));
            std::thread t2([](string_map &accum, string_map &val){accum.merge(std::move(val));},
                           std::ref(sbu), std::ref(sbu_map));
            std::thread t3([](metadata_map &accum, metadata_map &val){accum.merge(std::move(val));},
                           std::ref(prod_meta), std::ref(metadata));
            std::thread t4([](unordered_set<household_id > &accum, unordered_set<household_id> &val){accum.merge(std::move(val));},
                           std::ref(b_house), std::ref(black_house));
            std::thread t5([](unordered_set<person_id> &accum, unordered_set<person_id> &val){accum.merge(std::move(val));},
                           std::ref(b_person), std::ref(black_person));
            t0.join();
            t1.join();
            t2.join();
            t3.join();
            t4.join();
            t5.join();
        }
        return transaction_data;
    }
    vector<string> download_and_decompress(const vector<string> &remote_paths, const string save_to) {
        vector<std::future<string>> futures;
        vector<string> retval;
        auto download_and_decompress = [](const string &path, const string &dest_folder, const int seq) {
            std::system((string("aws s3 cp ") + path + string(" ") + dest_folder
                         + string("/") + std::to_string(seq) + string(".csv.lz4")).c_str());
            std::system((string("lz4 -d --rm -f " + dest_folder
                                + string("/") + std::to_string(seq) + string(".csv.lz4"))).c_str());
            return dest_folder + string("/") + std::to_string(seq) + string(".csv");
        };

        for (int i = 0; i < remote_paths.size(); ++i) {
            futures.push_back(std::async(std::launch::async, download_and_decompress, remote_paths[i], save_to, i));
        }
        wait_on_futures(futures);
        for (auto &f: futures) {
            retval.push_back(f.get());
        }
        return retval;
    }
}

