local urlparse = require("socket.url")
local http = require("socket.http")
local cjson = require("cjson")

local item_value = os.getenv('item_value')
local item_type = os.getenv('item_type')
local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

local discovered = {}
local forced_allowed = {}

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local ids = {}

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

allowed = function(url, parenturl)
  if string.match(url, "^https?://[^/]+/download_repair%.php")
    or string.match(url, "^https?://[^/]*facebook%.com/") then
    return false
  end

  if forced_allowed[url] then
    return true
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if tested[s] == nil then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end

  for s in string.gmatch(url, "[0-9a-zA-Z]+") do
    if ids[s] or ids[string.sub(s, 1, #s-2)] then
      return true
    end
  end

  if string.match(url, "^https?://[^/]*mediafire%.com/convkey/") then
    return true
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  local function check(urla)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.match(url_, "^(.-)%s*$")
    url_ = string.match(url_, "^(.-)%??$")
    url_ = string.match(url_, "^(.-)&?$")
    url_ = string.match(url_, "^(.-)/?$")
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"))
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, "/" .. newurl))
    end
  end

  local function json_get(json, s)
    if not json[s] then
      io.stdout:write("Could not find data in table.\n")
      io.stdout:flush()
      abortgrab = true
    end
    return json[s]
  end

  if status_code < 400 and allowed(url, nil) then
    local a, b = string.match(url, "^(https?://[^/]+/convkey/.+)[0-9a-z](g%.jpg)$")
    if a and b then
      for i = 0, 9 do
        check(a .. tostring(i) .. b)
      end
      for i = 97, 122 do
        check(a .. string.char(i) .. b)
      end
    end
    if (
        string.match(url, "^https?://www%.mediafire%.com/")
        or string.match(url, "^https?://mediafire%.com/")
      )
      and not string.match(url, "^https?://[^/]+/api/")
      and not string.match(url, "^https?://[^/]+/convkey/")
      and not string.match(url, "^https?://[^/]+/widgets/") then
      check(string.gsub(url, "^(https?://)[^/]+(/.+)$", "%1mfi.re%2"))
    end
  end

  if status_code >= 400 and status_code < 500 then
    local a, b = string.match(url, "^https?://www%.mediafire%.com/api/1%.5/([^/]+)/get_info%.php%?.+_key=([0-9a-zA-Z]+)")
    if a and b then
      if try_one_failed then
        abortgrab = true
      end
      try_one_failed = true
      if a == "file" then
        check("https://www.mediafire.com/api/1.5/folder/get_info.php?folder_key=" .. b .. "&response_format=json")
      elseif a == "folder" then
        check("https://www.mediafire.com/api/1.5/file/get_info.php?quick_key=" .. b .. "&response_format=json")
      end
    end
  end
  
  local function check_url_api(newurl, post_data)
    if type(post_data) ~= "string" then
      post_data = nil
    end
    if post_data ~= nil then
      table.insert(urls, {url=newurl, post_data=post_data})
    else
      check(newurl)
    end
    for _, replace_with in pairs({"/api/1%.5/", "/api/1%.4/"}) do
      local otherurl = string.gsub(newurl, "/api/1%.[45]/", replace_with)
      if otherurl ~= newurl then
        if post_data ~= nil then
          table.insert(urls, {url=otherurl, post_data=post_data})
        else
          check(newurl)
        end
      end
    end
  end

  if allowed(url, nil) and status_code == 200
    and not string.match(url, "^https?://download[0-9]*%.mediafire%.com/")
    and not string.match(url, "^https?://[^/]*mediafire%.com/convkey/") then
    html = read_file(file)
    local json = nil
    
    if string.match(url, "json$") or string.match(html, "^{") then
      json = cjson.decode(html)
      assert(json["response"]["result"] == "Success")
    end
    
    if string.match(url, "^https?://[^/]*mediafire%.com/api/.+&response_format=") then
      check_url_api(string.gsub(url, "(&response_format=)[a-z]+", "%1json"))
      check_url_api(string.gsub(url, "(&response_format=)[a-z]+", "%1xml"))
      check_url_api(string.gsub(url, "&response_format=[a-z]+", ""))
    end

    if string.match(url, "^https?://[^/]*mediafire%.com/api/1%.[0-9]+/.+%?") then
--[[      check_url_api(string.gsub(url, "(/api/1%.)[0-9]+", "%10"))
      check_url_api(string.gsub(url, "(/api/1%.)[0-9]+", "%11"))
      check_url_api(string.gsub(url, "(/api/1%.)[0-9]+", "%12"))
      check_url_api(string.gsub(url, "(/api/1%.)[0-9]+", "%13"))]]
      check_url_api(string.gsub(url, "(/api/1%.)[0-9]+", "%14"))
      check_url_api(string.gsub(url, "(/api/1%.)[0-9]+", "%15"))
    end

    local sort, match = string.match(url, "^https?://[^/]*mediafire%.com/api/1%.[0-9]+/([^/]+)/get_info%.php%?.+_key=([0-9a-zA-Z_%.]+)")
    if match then
      check_url_api("https://www.mediafire.com/?" .. match)
      check_url_api("https://www.mediafire.com/i/?" .. match)
      if sort == "folder" then
        --[[if true then
          io.stdout:write("Folder are not supported at this time.\n")
          io.stdout:flush()
          abortgrab = true
          return {}
        end]]
        check_url_api("https://www.mediafire.com/folder/" .. match)
        check_url_api("https://www.mediafire.com/api/1.5/folder/get_info.php?folder_key=" .. match .. "&response_format=json")
        check_url_api("https://www.mediafire.com/api/1.5/folder/get_info.php?folder_key=" .. match .. "&response_format=json&recursive=yes")
        check_url_api("https://www.mediafire.com/api/1.5/folder/get_info.php?folder_key=" .. match .. "&response_format=json&details=yes")
        check_url_api("https://www.mediafire.com/api/1.5/folder/get_content.php?content_type=folders&filter=all&order_by=name&order_direction=asc&chunk=1&version=1.5&folder_key=" .. match .. "&response_format=json")
        check_url_api("https://www.mediafire.com/api/1.5/folder/get_content.php?content_type=files&filter=all&order_by=name&order_direction=asc&chunk=1&version=1.5&folder_key=" .. match .. "&response_format=json")
      elseif sort == "file" then
        check_url_api("https://www.mediafire.com/file/" .. match)
        check_url_api("https://www.mediafire.com/file_premium/" .. match)
        check_url_api("https://www.mediafire.com/view/" .. match)
        check_url_api("https://www.mediafire.com/play/" .. match)
        check_url_api("https://www.mediafire.com/listen/" .. match)
        check_url_api("https://www.mediafire.com/watch/" .. match)
        check_url_api("https://www.mediafire.com/download/" .. match)
        check_url_api("https://www.mediafire.com/download.php?" .. match)
        check_url_api("https://www.mediafire.com/imageview.php?quickkey=" .. match) -- &thumb=
        check_url_api("https://www.mediafire.com/api/1.5/file/get_info.php?quick_key=" .. match .. "&response_format=json")
        check_url_api("https://www.mediafire.com/api/1.5/file/get_links.php?quick_key=" .. match .. "&response_format=json")
      end
    end

    local a, b = string.match(url, "^(https?://[^/]*mediafire%.com/api/.+%.php)%?(.+)$")
    if a and b then
      forced_allowed[a] = true
      check_url_api(a, b)
    end

    if string.match(url, "^https?://[^/]*/file/")
      and string.match(url, "/file$") then
      check_url_api(string.match(url, "^(.+)/file$"))
      check_url_api(string.gsub(url, "^(https?://[^/]*/)file(/?.+)/file$", "%1listen%2"))
      check_url_api(string.gsub(url, "^(https?://[^/]*/)file(/?.+)/file$", "%1view%2"))
      check_url_api(string.gsub(url, "^(https?://[^/]*/)file(/?.+)/file$", "%1watch%2"))
    end

    if string.match(url, "^https?://[^/]+/api/1%.[0-9]/.")
      and not string.match(url, "[%?&]response_format=json") then
      if not string.match(html, "<result>Success</result>")
        and not string.match(html, '"result":"Success"') then
        io.stdout:write("API request not succesful.\n")
        io.stdout:flush()
        abortgrab = true
        return urls
      end
    end

    if string.match(url, "^https?://[^/]+/api/1%.[0-9]/.")
      and string.match(html, "^{") then
      local json = cjson.decode(html)
      if not json then
        io.stdout:write("Invalid JSON response.\n")
        io.stdout:flush()
        abortgrab = true
      end

      json = json_get(json, "response")
      match = string.match(url, "/folder/get_info%..+[%?&]folder_key=([0-9a-zA-Z]+)")
      if match then
        j = json_get(json, "folder_info")
        if j["name"] then
          local name = string.gsub(j["name"], " ", "_")
          check_url_api("https://www.mediafire.com/?" .. match .. "/" .. name)
          check_url_api("https://www.mediafire.com/folder/" .. match .. "/" .. name)
        end
      end

      if string.match(url, "/folder/get_content.+[%?&]chunk=[0-9]+.*&response_format=json") then
        j = json_get(json, "folder_content")
        if j["more_chunks"] and j["more_chunks"] == "yes" then
          local chunk = string.match(url, "[%?&]chunk=([0-9]+)")
          check_url_api(string.gsub(url, "([%?&]chunk=)[0-9]+", "%1" .. tostring(tonumber(chunk)+1)))
        end
      end

      if string.match(url, "/folder/get_content%.php")
        and string.match(url, "[%?&]response_format=json") then
        j = json_get(json, "folder_content")
        local all_ids = ""
        local sort = string.match(url, "[%?&]content_type=([a-z]+)")
        if not sort then
          io.stdout:write("Could not determine sort.\n")
          io.stdout:flush()
          abortgrab = true
          return urls
        end
        local keyname = nil
        if sort == "files" then
          keyname = "quickkey"
        elseif sort == "folders" then
          keyname = "folderkey"
        else
          io.stdout:write("Type unsupported.\n")
          io.stdout:flush()
          abortgrab = true
          return urls
        end
        local more_chunks = j["more_chunks"]
        local actual_size = 0
        for _, d in pairs(json_get(j, sort)) do
          actual_size = actual_size + 1
          local new_id = json_get(d, keyname)
          if sort == "folders" then
            if string.len(all_ids) > 0 then
              all_ids = all_ids .. "%2C"
            end
            all_ids = all_ids .. new_id
          end
          discovered["id:" .. new_id] = true
        end
        if more_chunks == "yes" then
          local chunk_size = tonumber(j["chunk_size"])
          assert(chunk_size == actual_size)
          local chunk_number = tostring(tonumber(j["chunk_number"])+1)
          if string.match(url, "[%?&]chunk=[0-9]+") then
            check_url_api(string.gsub(url, "([%?&]chunk=)[0-9]+", "%1" .. chunk_number))
          else
            check_url_api(url .. "&chunk=" .. chunk_number)
          end
        end
        if sort == "folders" and string.len(all_ids) > 0 then
          local newurl = "https://www.mediafire.com/api/1.4/folder/get_info.php"
          forced_allowed[newurl] = true
          check_url_api(newurl, "folder_key=" .. all_ids  .. "&details=yes&response_format=yes")
        end
      end
    end

    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  local match = string.match(url["url"], "^https?://[^/]*mediafire%.com/api/1%.[45]/[^/]+/get_info%.php%?.+_key=([0-9a-zA-Z]+)")
  if match then
    ids[match] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if string.match(newloc, "inactive%.min")
      or string.match(newloc, "ReturnUrl")
      or string.match(newloc, "adultcontent") then
      io.stdout:write("Found invalid redirect.\n")
      io.stdout:flush()
      abortgrab = true
    end
    if downloaded[newloc] == true or addedtolist[newloc] == true
      or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  if abortgrab == true then
    io.stdout:write("ABORTING...\n")
    io.stdout:flush()
    return wget.actions.ABORT
  end

  if status_code == 0
    or (status_code > 400 and status_code ~= 404) then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 12
    if not allowed(url["url"], nil) then
      maxtries = 3
    end
    if tries >= maxtries then
      io.stdout:write("I give up...\n")
      io.stdout:flush()
      tries = 0
      if maxtries == 3 then
        return wget.actions.EXIT
      else
        return wget.actions.ABORT
      end
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(newurls, key)
    local tries = 0
    local maxtries = 10
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        newurls .. "\0"
      )
      print(body)
      if code == 200 then
        io.stdout:write("Submitted discovered URLs.\n")
        io.stdout:flush()
        break
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == maxtries then
      kill_grab()
    end
  end

  for key, data in pairs({
    ["mediafire-db23vdgfp6gfwzx"] = discovered
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))--, "on shard", shard)
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end

