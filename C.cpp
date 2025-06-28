#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <stdexcept> // For std::runtime_error
#include <sstream>   // For std::istringstream
#include <filesystem> // For std::filesystem::create_directories
#include <algorithm> // For std::transform, std::max
#include <functional> // For std::function

// Asio headers (standalone)
#include <asio.hpp>
#include <asio/error_code.hpp> // Explicitly include for asio::error_code

// RapidJSON headers
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h" // For GetParseError_En


using tcp = asio::ip::tcp;

// RapidJSON aliases
using rapidjson::Document;
using rapidjson::Value;
using rapidjson::Writer;
using rapidjson::StringBuffer;
using rapidjson::SizeType;
using rapidjson::kArrayType;
using rapidjson::kObjectType;


// Constants
const unsigned short PORT = 3013;
const std::string BASE_STORAGE_DIRECTORY = "./uploaded_files/";
const std::string FORMATS_DIRECTORY = "./formats/"; // New: Directory for format files
const int MAX_REQUEST_SIZE = 1024 * 1024; // 1MB max request size (headers + body)

// --- HTTP Utilities ---

// Structure to hold parsed HTTP request
struct HttpRequest {
    std::string method;
    std::string target;
    std::string version; // e.g., "HTTP/1.1"
    std::map<std::string, std::string> headers;
    std::string body;
    bool keep_alive = false; // Based on Connection header
};

// Function to parse a single HTTP header line (e.g., "Content-Type: application/json")
std::pair<std::string, std::string> parse_header_line(const std::string& line) {
    size_t colon_pos = line.find(':');
    if (colon_pos == std::string::npos) {
        return {"", ""}; // Invalid header
    }
    std::string name = line.substr(0, colon_pos);
    std::string value = line.substr(colon_pos + 1);

    // Trim whitespace
    name.erase(0, name.find_first_not_of(" \t\r\n"));
    name.erase(name.find_last_not_of(" \t\r\n") + 1);
    value.erase(0, value.find_first_not_of(" \t\r\n"));
    value.erase(value.find_last_not_of(" \t\r\n") + 1);

    // Convert header name to lowercase for case-insensitive lookup
    std::transform(name.begin(), name.end(), name.begin(), ::tolower);

    return {name, value};
}

// Function to read an HTTP request from the socket
HttpRequest read_http_request(tcp::socket& socket) {
    HttpRequest req;
    asio::streambuf buffer;
    std::istream is(&buffer);
    std::string line;
    asio::error_code ec;

    asio::read_until(socket, buffer, "\r\n\r\n", ec);
    if (ec && ec != asio::error::eof) {
        throw asio::system_error(ec, "read_until headers error");
    }

    std::getline(is, line);
    std::stringstream ss(line);
    ss >> req.method >> req.target >> req.version;

    while (std::getline(is, line) && !line.empty() && line != "\r") {
        auto header = parse_header_line(line);
        if (!header.first.empty()) {
            req.headers[header.first] = header.second;
        }
    }

    auto it_conn = req.headers.find("connection");
    if (it_conn != req.headers.end() && it_conn->second == "keep-alive") {
        req.keep_alive = true;
    }

    auto it_cl = req.headers.find("content-length");
    if (it_cl != req.headers.end()) {
        try {
            size_t content_length = std::stoul(it_cl->second);
            if (content_length > MAX_REQUEST_SIZE) {
                throw std::runtime_error("Request body too large.");
            }
            size_t bytes_in_buffer = buffer.size();
            if (bytes_in_buffer < content_length) {
                asio::read(socket, buffer, asio::transfer_exactly(content_length - bytes_in_buffer), ec);
                if (ec && ec != asio::error::eof) {
                    throw asio::system_error(ec, "read body error");
                }
            }

            req.body.resize(content_length);
            is.read(&req.body[0], content_length);

        } catch (const std::exception& e) {
            throw std::runtime_error("Failed to read body or invalid Content-Length: " + std::string(e.what()));
        }
    }

    return req;
}

std::string get_status_message(int status_code) {
    switch (status_code) {
        case 200: return "OK";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 500: return "Internal Server Error";
        default: return "Unknown";
    }
}

void write_http_response(tcp::socket& socket, int status_code, const std::string& status_message, const std::string& content_type, const std::string& body, bool keep_alive, const std::map<std::string, std::string>& extra_headers = {}) {
    std::ostringstream oss;
    oss << "HTTP/1.1 " << status_code << " " << status_message << "\r\n";
    oss << "Content-Type: " << content_type << "\r\n";
    oss << "Content-Length: " << body.length() << "\r\n";
    oss << "Connection: " << (keep_alive ? "keep-alive" : "close") << "\r\n";

    oss << "Access-Control-Allow-Origin: http://localhost:3000\r\n";
    oss << "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n";
    oss << "Access-Control-Allow-Headers: Content-Type\r\n";
    oss << "Access-Control-Allow-Credentials: true\r\n";

    for (const auto& header : extra_headers) {
        oss << header.first << ": " << header.second << "\r\n";
    }

    oss << "\r\n";
    oss << body;

    std::string response_str = oss.str();
    asio::error_code ec;
    asio::write(socket, asio::buffer(response_str), ec);
    if (ec) {
        throw asio::system_error(ec, "write response error");
    }
}

// --- API Logic Helpers ---

std::string create_api_response(const std::string& status, const std::string& message, const Value* data = nullptr) {
    Document doc;
    doc.SetObject();
    Document::AllocatorType& allocator = doc.GetAllocator();

    doc.AddMember("status", Value().SetString(status.c_str(), allocator), allocator);
    doc.AddMember("message", Value().SetString(message.c_str(), allocator), allocator);

    if (data && !data->IsNull()) {
        doc.AddMember("data", Value(*data, allocator), allocator);
    }

    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    doc.Accept(writer);
    return buffer.GetString();
}

std::string read_file(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open file: " + path);
    }
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    return content;
}

// Overload for write_json_to_file that takes an existing Document
void write_json_to_file(const std::string& path, const rapidjson::Document& doc) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    doc.Accept(writer);

    std::ofstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open file for writing: " + path);
    }
    file << buffer.GetString();
    file.close();
}

// New overload for write_json_to_file to create content on the fly (for format files)
void write_json_to_file(const std::string& path, std::function<void(Document&, Document::AllocatorType&)> creator_func) {
    Document doc;
    doc.SetObject(); // Start with an empty object for new doc creation
    Document::AllocatorType& allocator = doc.GetAllocator();
    creator_func(doc, allocator); // Populate document using the lambda

    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    doc.Accept(writer);

    std::ofstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open file for writing format: " + path);
    }
    file << buffer.GetString();
    file.close();
}


int get_max_id(const rapidjson::Value& array) {
    int max_id = 0;
    if (array.IsArray()) {
        for (rapidjson::SizeType i = 0; i < array.Size(); ++i) {
            const rapidjson::Value& item = array[i];
            if (item.HasMember("id") && item["id"].IsInt()) {
                if (item["id"].GetInt() > max_id) {
                    max_id = item["id"].GetInt();
                }
            }
        }
    }
    return max_id;
}

rapidjson::Value merge_json_objects(const rapidjson::Value& obj1, const rapidjson::Value& obj2, rapidjson::Document::AllocatorType& allocator) {
    rapidjson::Value merged_obj(kObjectType);
    
    for (Value::ConstMemberIterator m = obj1.MemberBegin(); m != obj1.MemberEnd(); ++m) {
        merged_obj.AddMember(Value(m->name, allocator).Move(), Value(m->value, allocator).Move(), allocator);
    }
    for (Value::ConstMemberIterator m = obj2.MemberBegin(); m != obj2.MemberEnd(); ++m) {
        if (merged_obj.HasMember(m->name)) {
            merged_obj.RemoveMember(m->name);
        }
        merged_obj.AddMember(Value(m->name, allocator).Move(), Value(m->value, allocator).Move(), allocator);
    }
    return merged_obj;
}

// --- New Format Handling Logic ---

// New struct to hold file format information
struct DataFileFormat {
    bool root_is_array;
    std::string array_key; // Empty if root_is_array is true
};

// Global map to store loaded formats
std::map<std::string, DataFileFormat> g_data_formats;

// Function to load a single format file
DataFileFormat load_single_format_file(const std::string& path) {
    std::string content = read_file(path);
    Document doc;
    doc.Parse(content.c_str());

    if (doc.HasParseError()) {
        throw std::runtime_error("Error parsing format file " + path + ": " + rapidjson::GetParseError_En(doc.GetParseError()));
    }

    if (!doc.HasMember("root_is_array") || !doc["root_is_array"].IsBool()) {
        throw std::runtime_error("Format file " + path + " missing 'root_is_array' boolean.");
    }
    bool root_is_array = doc["root_is_array"].GetBool();

    std::string array_key = "";
    if (!root_is_array) {
        if (!doc.HasMember("array_key") || !doc["array_key"].IsString()) {
            throw std::runtime_error("Format file " + path + " missing 'array_key' string when 'root_is_array' is false.");
        }
        array_key = doc["array_key"].GetString();
    }

    return {root_is_array, array_key};
}

// Function to load all format definitions at startup
void load_all_data_formats() {
    std::filesystem::create_directories(FORMATS_DIRECTORY); // Ensure formats directory exists

    // MarketProduct
    write_json_to_file(FORMATS_DIRECTORY + "F_MarketProduct.json",
        [](Document& doc, Document::AllocatorType& allocator) {
            doc.SetObject();
            doc.AddMember("root_is_array", false, allocator);
            doc.AddMember("array_key", Value("products", allocator).Move(), allocator);
        });
    g_data_formats["MarketProduct"] = load_single_format_file(FORMATS_DIRECTORY + "F_MarketProduct.json");

    // StoreProduct
    write_json_to_file(FORMATS_DIRECTORY + "F_StoreProduct.json",
        [](Document& doc, Document::AllocatorType& allocator) {
            doc.SetObject();
            doc.AddMember("root_is_array", false, allocator);
            doc.AddMember("array_key", Value("products", allocator).Move(), allocator); // Empty key if root is array
        });
    g_data_formats["StoreProduct"] = load_single_format_file(FORMATS_DIRECTORY + "F_StoreProduct.json");

    // News
    write_json_to_file(FORMATS_DIRECTORY + "F_News.json",
        [](Document& doc, Document::AllocatorType& allocator) {
            doc.SetObject();
            doc.AddMember("root_is_array", false, allocator);
            doc.AddMember("array_key", Value("articles", allocator).Move(), allocator);
        });
    g_data_formats["News"] = load_single_format_file(FORMATS_DIRECTORY + "F_News.json");

    // User@Account
    write_json_to_file(FORMATS_DIRECTORY + "F_User@Account.json",
        [](Document& doc, Document::AllocatorType& allocator) {
            doc.SetObject();
            doc.AddMember("root_is_array", false, allocator);
            doc.AddMember("array_key", Value("Accounts", allocator).Move(), allocator);
        });
    g_data_formats["User@Account"] = load_single_format_file(FORMATS_DIRECTORY + "F_User@Account.json");

    // Placeholder format files for Account content structure
    write_json_to_file(FORMATS_DIRECTORY + "F_Account.json",
        [](Document& doc, Document::AllocatorType& allocator) {
            doc.SetObject();
            doc.AddMember("id", "string", allocator);
            doc.AddMember("username", "string", allocator);
            doc.AddMember("email", "string", allocator);
            doc.AddMember("password_hash", "string", allocator);
            doc.AddMember("full_name", "string", allocator);
            doc.AddMember("created_at", "string_datetime", allocator);
            doc.AddMember("last_login", "string_datetime", allocator);
            doc.AddMember("is_active", "boolean", allocator);
            doc.AddMember("roles", "array_of_strings", allocator);
            doc.AddMember("Account_Type", "string", allocator);
            doc.AddMember("Member_Ship", "string", allocator);
        });
    std::cout << "[+] : Loaded data file formats successfully from " << FORMATS_DIRECTORY << std::endl;
}


// --- Main API Logic Handler ---

std::string handle_api_request(const std::string& request_body_str, int& http_status_code) {
    Document request_doc;
    request_doc.Parse(request_body_str.c_str());

    http_status_code = 200;

    if (request_doc.HasParseError()) {
        http_status_code = 400;
        return create_api_response("error", "Invalid JSON in request body.");
    }

    if (!request_doc.HasMember("Method") || !request_doc["Method"].IsString()) {
        http_status_code = 400;
        return create_api_response("error", "Missing or invalid 'Method' field in JSON request.");
    }

    std::string api_method = request_doc["Method"].GetString();
    std::string api_type;
    std::string file_path_str;
    const Value* data_id_val = nullptr;
    std::string post_data_id_str;

    if (request_doc.HasMember("Type") && request_doc["Type"].IsString()) {
        api_type = request_doc["Type"].GetString();
    }
    if (request_doc.HasMember("file")) {
        const Value& file_val = request_doc["file"];
        if (file_val.IsString()) {
            file_path_str = file_val.GetString();
        } else if (file_val.IsArray() && file_val.Size() == 2 &&
                   file_val[0].IsString() && file_val[1].IsString()) {
            file_path_str = std::string(file_val[0].GetString()) + "/" + file_val[1].GetString();
        }
    }

    if (api_method == "POST") {
        if (request_doc.HasMember("Data_ID") && request_doc["Data_ID"].IsString()) {
            post_data_id_str = request_doc["Data_ID"].GetString();
        } else {
            http_status_code = 400;
            return create_api_response("error", "Missing or invalid 'Data_ID' for POST. Expected 'auto'.");
        }
    } else { // GET, PUT, DELETE
        if (request_doc.HasMember("Data_ID")) {
            data_id_val = &request_doc["Data_ID"];
        }
    }


    // --- Common logic for identifying target array using loaded formats ---
    const DataFileFormat* format = nullptr;
    auto it_format = g_data_formats.find(api_type);
    if (it_format != g_data_formats.end()) {
        format = &it_format->second;
    }

    if (!format) {
        http_status_code = 400;
        return create_api_response("error", "Unknown or unsupported API Type for file operations: " + api_type);
    }
    // --- End common logic ---


    if (api_method == "GET") {
        if (file_path_str.empty()) {
            http_status_code = 400;
            return create_api_response("error", "Filename not specified for GET operation.");
        }
        if (data_id_val == nullptr) {
             http_status_code = 400;
             return create_api_response("error", "Data_ID not specified for GET operation.");
        }

        std::string full_path = BASE_STORAGE_DIRECTORY + file_path_str;
        try {
            std::string file_content;
            if (!std::filesystem::exists(full_path)) {
                http_status_code = 404;
                return create_api_response("error", "Target file not found: " + full_path);
            }
            file_content = read_file(full_path);
            
            Document file_doc;
            file_doc.Parse(file_content.c_str());

            if (file_doc.HasParseError()) {
                http_status_code = 500;
                return create_api_response("error", "Could not parse JSON from file: " + full_path);
            }

            Value* target_array = nullptr;

            if (format->root_is_array) {
                if (file_doc.IsArray()) {
                    target_array = &file_doc;
                } else { // File exists but is not an array as expected
                    http_status_code = 400;
                    return create_api_response("error", "File for Type '" + api_type + "' is not a JSON array as expected.");
                }
            } else { // Root is an object with an array_key
                if (file_doc.IsObject() && file_doc.HasMember(format->array_key.c_str()) && file_doc[format->array_key.c_str()].IsArray()) {
                    target_array = &file_doc[format->array_key.c_str()];
                } else { // File exists but doesn't have the expected object/array structure
                    http_status_code = 400;
                    return create_api_response("error", "File for Type '" + api_type + "' does not contain expected object/array structure ('" + format->array_key + "').");
                }
            }

            if (!target_array) { // Fallback, should ideally not happen if format is valid
                 http_status_code = 500;
                 return create_api_response("error", "Internal error: Target array could not be identified for GET.");
            }


            if (data_id_val->IsString() && std::string(data_id_val->GetString()) == "ALL") {
                return create_api_response("success", "Data retrieved successfully.", &file_doc); // Return whole doc for ALL
            } else if (data_id_val->IsInt()) {
                int id_to_find = data_id_val->GetInt();
                Value found_item_val;

                for (SizeType i = 0; i < target_array->Size(); ++i) {
                    const Value& item = (*target_array)[i];
                    if (item.HasMember("id") && item["id"].IsInt() && item["id"].GetInt() == id_to_find) {
                        found_item_val.CopyFrom(item, file_doc.GetAllocator());
                        break;
                    }
                }

                if (!found_item_val.IsNull()) {
                    return create_api_response("success", "Item retrieved successfully.", &found_item_val);
                } else {
                    http_status_code = 404;
                    return create_api_response("error", "Item with Data_ID " + std::to_string(id_to_find) + " not found.");
                }
            } else {
                http_status_code = 400;
                return create_api_response("error", "Invalid Data_ID format for GET operation. Expected 'ALL' or a number.");
            }
        } catch (const std::exception& e) {
            http_status_code = 500;
            return create_api_response("error", "Server error during GET operation: " + std::string(e.what()));
        }
    } else if (api_method == "POST") {
        if (post_data_id_str != "auto") {
            http_status_code = 400;
            return create_api_response("error", "Invalid 'Data_ID' for POST. Expected 'auto'.");
        }
        if (file_path_str.empty()) {
            http_status_code = 400;
            return create_api_response("error", "Filename not specified for POST operation.");
        }
        if (!request_doc.HasMember("Surface_content") || !request_doc["Surface_content"].IsString() ||
            !request_doc.HasMember("Main_content") || !request_doc["Main_content"].IsString()) {
            http_status_code = 400;
            return create_api_response("error", "Missing 'Surface_content' or 'Main_content' for POST operation.");
        }
        
        std::string surface_content_str = request_doc["Surface_content"].GetString();
        std::string main_content_str = request_doc["Main_content"].GetString();

        Document surface_doc, main_doc;
        surface_doc.Parse(surface_content_str.c_str());
        main_doc.Parse(main_content_str.c_str());

        if (surface_doc.HasParseError()) {
            http_status_code = 400;
            return create_api_response("error", "Invalid JSON in 'Surface_content': " + std::string(rapidjson::GetParseError_En(surface_doc.GetParseError())) + " at offset " + std::to_string(surface_doc.GetErrorOffset()));
        }
        if (main_doc.HasParseError()) {
            http_status_code = 400;
            return create_api_response("error", "Invalid JSON in 'Main_content': " + std::string(rapidjson::GetParseError_En(main_doc.GetParseError())) + " at offset " + std::to_string(main_doc.GetErrorOffset()));
        }
        if (!surface_doc.IsObject() || !main_doc.IsObject()) {
            http_status_code = 400;
            return create_api_response("error", "Content must be JSON objects in 'Surface_content' and 'Main_content'.");
        }

        std::string full_path = BASE_STORAGE_DIRECTORY + file_path_str;
        try {
            std::string file_content;
            if (std::filesystem::exists(full_path)) {
                file_content = read_file(full_path);
            }
            
            Document target_file_doc;
            if (!file_content.empty()) {
                target_file_doc.Parse(file_content.c_str());
            }

            // --- Dynamic target_array identification for POST ---
            Value* target_array = nullptr;
            if (format->root_is_array) {
                if (!target_file_doc.IsArray()) { // Initialize if not already an array or empty/corrupted
                    target_file_doc.SetArray();
                }
                target_array = &target_file_doc;
            } else { // Root is an object with an array_key
                if (!target_file_doc.IsObject()) { // Initialize if not an object or empty/corrupted
                    target_file_doc.SetObject();
                }
                if (!target_file_doc.HasMember(format->array_key.c_str()) || !target_file_doc[format->array_key.c_str()].IsArray()) {
                    target_file_doc.AddMember(Value(format->array_key.c_str(), target_file_doc.GetAllocator()).Move(), Value(kArrayType).Move(), target_file_doc.GetAllocator());
                }
                target_array = &target_file_doc[format->array_key.c_str()];
            }
            // --- End Dynamic target_array identification ---

            if (!target_array) {
                http_status_code = 500;
                return create_api_response("error", "Target array not found or invalid structure for POST. File: " + full_path);
            }
            
            Value new_item = merge_json_objects(surface_doc, main_doc, target_file_doc.GetAllocator());
            
            int new_id = get_max_id(*target_array) + 1;
            new_item.AddMember("id", new_id, target_file_doc.GetAllocator());
            target_array->PushBack(new_item, target_file_doc.GetAllocator());

            write_json_to_file(full_path, target_file_doc);
            std::cout << "[+] : Data saved to " << full_path << " with ID: " << new_item["id"].GetInt() << std::endl;
            return create_api_response("success", "Data saved successfully.", &new_item);

        } catch (const std::exception& e) {
            http_status_code = 500;
            return create_api_response("error", "Server error during POST operation: " + std::string(e.what()));
        }
    } else if (api_method == "PUT") {
        if (file_path_str.empty()) {
            http_status_code = 400;
            return create_api_response("error", "Filename not specified for PUT operation.");
        }
        if (data_id_val == nullptr || !data_id_val->IsInt()) {
            http_status_code = 400;
            return create_api_response("error", "Missing or invalid 'Data_ID' for PUT operation. Expected an integer ID.");
        }
        if (!request_doc.HasMember("Surface_content") || !request_doc["Surface_content"].IsString() ||
            !request_doc.HasMember("Main_content") || !request_doc["Main_content"].IsString()) {
            http_status_code = 400;
            return create_api_response("error", "Missing 'Surface_content' or 'Main_content' for PUT operation (contains update data).");
        }

        std::string surface_content_str = request_doc["Surface_content"].GetString();
        std::string main_content_str = request_doc["Main_content"].GetString();

        Document surface_doc, main_doc;
        surface_doc.Parse(surface_content_str.c_str());
        main_doc.Parse(main_content_str.c_str());

        if (surface_doc.HasParseError()) {
            http_status_code = 400;
            return create_api_response("error", "Invalid JSON in 'Surface_content' for PUT: " + std::string(rapidjson::GetParseError_En(surface_doc.GetParseError())) + " at offset " + std::to_string(surface_doc.GetErrorOffset()));
        }
        if (main_doc.HasParseError()) {
            http_status_code = 400;
            return create_api_response("error", "Invalid JSON in 'Main_content' for PUT: " + std::string(rapidjson::GetParseError_En(main_doc.GetParseError())) + " at offset " + std::to_string(main_doc.GetErrorOffset()));
        }
        if (!surface_doc.IsObject() || !main_doc.IsObject()) {
            http_status_code = 400;
            return create_api_response("error", "Content must be JSON objects in 'Surface_content' and 'Main_content' for PUT.");
        }

        int id_to_update = data_id_val->GetInt();
        std::string full_path = BASE_STORAGE_DIRECTORY + file_path_str;

        try {
            if (!std::filesystem::exists(full_path)) {
                http_status_code = 404;
                return create_api_response("error", "Target file not found for PUT: " + full_path);
            }

            std::string file_content = read_file(full_path);
            Document target_file_doc;
            target_file_doc.Parse(file_content.c_str());

            if (target_file_doc.HasParseError()) {
                http_status_code = 500;
                return create_api_response("error", "Could not parse JSON from file for PUT: " + full_path);
            }

            // --- Dynamic target_array identification for PUT ---
            Value* target_array = nullptr;
            if (format->root_is_array) {
                if (!target_file_doc.IsArray()) { // Initialize if not an array (e.g. empty or corrupted file)
                    target_file_doc.SetArray();
                }
                target_array = &target_file_doc;
            } else { // Root is an object with an array_key
                if (!target_file_doc.IsObject()) {
                    target_file_doc.SetObject();
                }
                if (!target_file_doc.HasMember(format->array_key.c_str()) || !target_file_doc[format->array_key.c_str()].IsArray()) {
                    target_file_doc.AddMember(Value(format->array_key.c_str(), target_file_doc.GetAllocator()).Move(), Value(kArrayType).Move(), target_file_doc.GetAllocator());
                }
                target_array = &target_file_doc[format->array_key.c_str()];
            }
            // --- End Dynamic target_array identification ---

            if (!target_array) {
                http_status_code = 500;
                return create_api_response("error", "Internal server error: Target array could not be identified for PUT.");
            }

            bool item_updated = false;
            Value updated_item_val; // To hold the final updated item for response

            for (SizeType i = 0; i < target_array->Size(); ++i) {
                Value& current_item = (*target_array)[i];
                if (current_item.HasMember("id") && current_item["id"].IsInt() && current_item["id"].GetInt() == id_to_update) {
                    // Merge existing item with new content from Surface_content and Main_content
                    Value temp_merged = merge_json_objects(current_item, surface_doc, target_file_doc.GetAllocator());
                    Value final_merged = merge_json_objects(temp_merged, main_doc, target_file_doc.GetAllocator());
                    
                    // Ensure the ID is preserved and correct in the merged object
                    if (!final_merged.HasMember("id")) {
                        final_merged.AddMember("id", id_to_update, target_file_doc.GetAllocator());
                    } else if (final_merged["id"].IsInt() && final_merged["id"].GetInt() != id_to_update) {
                        // If 'id' was somehow changed in content, restore it
                        final_merged["id"].SetInt(id_to_update);
                    }

                    // Replace the old item with the new, merged item in the array
                    current_item.CopyFrom(final_merged, target_file_doc.GetAllocator());
                    updated_item_val.CopyFrom(current_item, target_file_doc.GetAllocator()); // Copy for the response
                    item_updated = true;
                    break;
                }
            }

            if (!item_updated) {
                http_status_code = 404; // Not Found
                return create_api_response("error", "Item with Data_ID " + std::to_string(id_to_update) + " not found in " + api_type + " for update.");
            }

            // Write back the modified document to the file
            write_json_to_file(full_path, target_file_doc);
            std::cout << "[+] : Item with ID " << id_to_update << " updated in " << full_path << std::endl;
            return create_api_response("success", "Item updated successfully.", &updated_item_val);

        } catch (const std::exception& e) {
            http_status_code = 500;
            return create_api_response("error", "Server error during PUT operation: " + std::string(e.what()));
        }
    } else if (api_method == "DELETE") {
        if (file_path_str.empty()) {
            http_status_code = 400;
            return create_api_response("error", "Filename not specified for DELETE operation.");
        }
        if (data_id_val == nullptr) {
            http_status_code = 400;
            return create_api_response("error", "Data_ID not specified for DELETE operation. Expected 'ALL' or a number.");
        }

        std::string full_path = BASE_STORAGE_DIRECTORY + file_path_str;
        try {
            if (!std::filesystem::exists(full_path)) {
                http_status_code = 404;
                return create_api_response("error", "Target file not found: " + full_path);
            }

            std::string file_content = read_file(full_path);
            Document target_file_doc;
            target_file_doc.Parse(file_content.c_str());

            if (target_file_doc.HasParseError()) {
                http_status_code = 500;
                return create_api_response("error", "Could not parse JSON from file for DELETE: " + full_path);
            }

            // --- Dynamic target_array identification for DELETE ---
            Value* target_array = nullptr;
            if (format->root_is_array) {
                if (target_file_doc.IsArray()) {
                    target_array = &target_file_doc;
                } else { // File exists but is not an array as expected
                    http_status_code = 400;
                    return create_api_response("error", "File for Type '" + api_type + "' is not a JSON array as expected.");
                }
            } else { // Root is an object with an array_key
                if (target_file_doc.IsObject() && target_file_doc.HasMember(format->array_key.c_str()) && target_file_doc[format->array_key.c_str()].IsArray()) {
                    target_array = &target_file_doc[format->array_key.c_str()];
                } else { // File exists but doesn't have the expected object/array structure
                    http_status_code = 400;
                    return create_api_response("error", "File for Type '" + api_type + "' does not contain expected object/array structure ('" + format->array_key + "').");
                }
            }
            // --- End Dynamic target_array identification ---

            if (!target_array) {
                 http_status_code = 500;
                 return create_api_response("error", "Internal server error: Target array could not be identified for DELETE.");
            }

            bool item_deleted = false;
            std::string delete_message = "";

            if (data_id_val->IsString() && std::string(data_id_val->GetString()) == "ALL") {
                if (target_array->IsArray()) {
                    target_array->Clear();
                    item_deleted = true;
                    delete_message = "All items deleted from " + api_type;
                } else {
                    http_status_code = 400;
                    return create_api_response("error", "Cannot delete 'ALL': target for Type '" + api_type + "' is not a JSON array.");
                }
            } else if (data_id_val->IsInt()) {
                int id_to_delete = data_id_val->GetInt();
                for (SizeType i = 0; i < target_array->Size(); ++i) {
                    Value& item = (*target_array)[i];
                    if (item.HasMember("id") && item["id"].IsInt() && item["id"].GetInt() == id_to_delete) {
                        target_array->Erase(target_array->Begin() + i);
                        item_deleted = true;
                        delete_message = "Item with ID " + std::to_string(id_to_delete) + " deleted from " + api_type;
                        break;
                    }
                }

                if (!item_deleted) {
                    http_status_code = 404;
                    return create_api_response("error", "Item with Data_ID " + std::to_string(id_to_delete) + " not found in " + api_type + ".");
                }
            } else {
                http_status_code = 400;
                return create_api_response("error", "Invalid Data_ID format for DELETE operation. Expected 'ALL' or a number.");
            }

            write_json_to_file(full_path, target_file_doc);
            std::cout << "[+] : " << delete_message << ". File: " << full_path << std::endl;
            return create_api_response("success", delete_message);

        } catch (const std::exception& e) {
            http_status_code = 500;
            return create_api_response("error", "Server error during DELETE operation: " + std::string(e.what()));
        }
    }
    else {
        http_status_code = 400;
        return create_api_response("error", "Unknown 'Method' specified in JSON request: " + api_method);
    }
}

// Handles a single client session
void do_session(tcp::socket& socket) {
    try {
        HttpRequest req = read_http_request(socket);

        std::cout << "[+] : Client Connected: " << socket.remote_endpoint() << std::endl;
        std::cout << "[+] : -> Request: " << req.method << " " << req.target << std::endl;
        std::cout << "[+] : -> Body: " << req.body << std::endl;

        std::string response_body_json;
        int http_status_code;

        if (req.method == "OPTIONS") {
            write_http_response(socket, 200, get_status_message(200), "", "", req.keep_alive);
            return;
        }

        if (req.target == "/") {
            response_body_json = handle_api_request(req.body, http_status_code);
        } else {
            http_status_code = 404;
            response_body_json = create_api_response("error", "Resource not found.");
        }

        write_http_response(socket, http_status_code, get_status_message(http_status_code), "application/json", response_body_json, req.keep_alive);

    } catch (const asio::system_error& e) {
        if (e.code() == asio::error::eof || e.code() == asio::error::connection_reset) {
             std::cerr << "[-] : Client connection closed or reset." << std::endl;
        } else {
            std::cerr << "[-] : Session asio::system_error: " << e.what() << " (" << e.code() << ")" << std::endl;
            try {
                write_http_response(socket, 500, get_status_message(500), "application/json",
                                    create_api_response("error", "Server error (asio): " + std::string(e.what())), false);
            } catch (const std::exception& e_write) {
                std::cerr << "[-] : Failed to send error response during asio error: " << e_write.what() << std::endl;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "[-] : Session std::exception: " << e.what() << std::endl;
        try {
            write_http_response(socket, 500, get_status_message(500), "application/json",
                                create_api_response("error", "Server error: " + std::string(e.what())), false);
        } catch (const std::exception& e_write) {
            std::cerr << "[-] : Failed to send error response: " << e_write.what() << std::endl;
        }
    }

    asio::error_code ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_send, ec);
    socket.close(ec);
    std::cout << "[-] : Client disconnected" << std::endl;
}

// Main function
int main() {
    asio::io_context ioc{1};

    try {
        std::filesystem::create_directories(BASE_STORAGE_DIRECTORY + "News");
        std::filesystem::create_directories(BASE_STORAGE_DIRECTORY + "Market");
        std::filesystem::create_directories(BASE_STORAGE_DIRECTORY + "Store");
        std::filesystem::create_directories(BASE_STORAGE_DIRECTORY + "Account");
        std::cout << "[+] : Base storage directory exists: " << BASE_STORAGE_DIRECTORY << std::endl;

        load_all_data_formats(); // Load formats at startup

    } catch (const std::exception& e) {
        std::cerr << "[-] : Error setting up directories or loading formats: " << e.what() << std::endl;
        return 1;
    }

    tcp::acceptor acceptor{ioc, {tcp::v4(), PORT}};
    std::cout << "[+] : HTTP Server Listening at port :" << PORT << std::endl;

    while (true) {
        tcp::socket socket{ioc};
        acceptor.accept(socket);
        do_session(socket);
    }

    return 0;
}