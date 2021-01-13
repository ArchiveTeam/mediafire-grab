dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = assert(loadfile "JSON.lua")()

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

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

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
  if string.match(urlparse.unescape(url), "[<>\\%*%$%^%[%],%(%){}]")
    or string.match(url, "^https?://[^/]+/download_repair%.php")
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
    if (string.match(url, "^https?://www%.mediafire%.com/")
      or string.match(url, "^https?://mediafire%.com/"))
      and not string.match(url, "^https?://[^/]+/api/")
      and not string.match(url, "^https?://[^/]+/convkey/")
      and not string.match(url, "^https?://[^/]+/widgets/") then
      check(string.gsub(url, "^(https?://)[^/]+(/.+)$", "%1mfi.re%2"))
    end
  end

  if status_code >= 400 and status_code < 500 then
    local a, b = string.match(url, "^https?://www%.mediafire%.com/api/1%.5/([^/]+)/get_info%.php%?.+_key=([0-9a-zA-Z]+)")
    if a and b then
      if a == "file" then
        check("https://www.mediafire.com/api/1.5/folder/get_info.php?folder_key=" .. b .. "&response_format=json")
      elseif a == "folder" then
        check("https://www.mediafire.com/api/1.5/file/get_info.php?quick_key=" .. b .. "&response_format=json")
      end
    end
  end

  if allowed(url, nil) and status_code == 200
    and not string.match(url, "^https?://download[0-9]*%.mediafire%.com/")
    and not string.match(url, "^https?://[^/]*mediafire%.com/convkey/") then
    html = read_file(file)
    if string.match(url, "^https?://[^/]*mediafire%.com/api/.+&response_format=") then
      check(string.gsub(url, "(&response_format=)[a-z]+", "%1json"))
      check(string.gsub(url, "(&response_format=)[a-z]+", "%1xml"))
      check(string.gsub(url, "&response_format=[a-z]+", ""))
    end

    if string.match(url, "^https?://[^/]*mediafire%.com/api/1%.[0-9]+/.+%?") then
--[[      check(string.gsub(url, "(/api/1%.)[0-9]+", "%10"))
      check(string.gsub(url, "(/api/1%.)[0-9]+", "%11"))
      check(string.gsub(url, "(/api/1%.)[0-9]+", "%12"))
      check(string.gsub(url, "(/api/1%.)[0-9]+", "%13"))]]
      check(string.gsub(url, "(/api/1%.)[0-9]+", "%14"))
      check(string.gsub(url, "(/api/1%.)[0-9]+", "%15"))
    end

    local sort, match = string.match(url, "^https?://[^/]*mediafire%.com/api/1%.[0-9]+/([^/]+)/get_info%.php%?.+_key=([0-9a-zA-Z_%.]+)")
    if match then
      check("https://www.mediafire.com/?" .. match)
      check("https://www.mediafire.com/i/?" .. match)
      if sort == "folder" then
        if true then
          io.stdout:write("Folder are not supported at this time.\n")
          io.stdout:flush()
          abortgrab = true
          return {}
        end
        check("https://www.mediafire.com/folder/" .. match)
        check("https://www.mediafire.com/api/1.5/folder/get_info.php?folder_key=" .. match .. "&response_format=json")
        check("https://www.mediafire.com/api/1.5/folder/get_info.php?folder_key=" .. match .. "&response_format=json&recursive=yes")
        check("https://www.mediafire.com/api/1.5/folder/get_info.php?folder_key=" .. match .. "&response_format=json&details=yes")
        check("https://www.mediafire.com/api/1.5/folder/get_content.php?content_type=folders&filter=all&order_by=name&order_direction=asc&chunk=1&version=1.5&folder_key=" .. match .. "&response_format=json")
        check("https://www.mediafire.com/api/1.5/folder/get_content.php?content_type=files&filter=all&order_by=name&order_direction=asc&chunk=1&version=1.5&folder_key=" .. match .. "&response_format=json")
      elseif sort == "file" then
        check("https://www.mediafire.com/file/" .. match)
        check("https://www.mediafire.com/view/" .. match)
        check("https://www.mediafire.com/play/" .. match)
        check("https://www.mediafire.com/listen/" .. match)
        check("https://www.mediafire.com/watch/" .. match)
        check("https://www.mediafire.com/download/" .. match)
        check("https://www.mediafire.com/download.php?" .. match)
        check("https://www.mediafire.com/imageview.php?quickkey=" .. match) -- &thumb=
        check("https://www.mediafire.com/api/1.5/file/get_info.php?quick_key=" .. match .. "&response_format=json")
        check("https://www.mediafire.com/api/1.5/file/get_links.php?quick_key=" .. match .. "&response_format=json")
      end
    end

    local a, b = string.match(url, "^(https?://[^/]*mediafire%.com/api/.+%.php)%?(.+)$")
    if a and b then
      forced_allowed[a] = true
      table.insert(urls, { url=a, post_data=b })
    end

    if string.match(url, "^https?://[^/]*/file/")
      and string.match(url, "/file$") then
      check(string.match(url, "^(.+)/file$"))
      check(string.gsub(url, "^(https?://[^/]*/)file(/?.+)/file$", "%1listen%2"))
      check(string.gsub(url, "^(https?://[^/]*/)file(/?.+)/file$", "%1view%2"))
      check(string.gsub(url, "^(https?://[^/]*/)file(/?.+)/file$", "%1watch%2"))
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
      local json = JSON:decode(html)
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
          check("https://www.mediafire.com/?" .. match .. "/" .. name)
          check("https://www.mediafire.com/folder/" .. match .. "/" .. name)
        end
      end

      if string.match(url, "/folder/get_content.+[%?&]chunk=[0-9]+.*&response_format=json") then
        j = json_get(json, "folder_content")
        if j["more_chunks"] and j["more_chunks"] == "yes" then
          local chunk = string.match(url, "[%?&]chunk=([0-9]+)")
          check(string.gsub(url, "([%?&]chunk=)[0-9]+", "%1" .. tostring(tonumber(chunk)+1)))
        end
      end

      if string.match(url, "/folder/get_content%.php")
        and string.match(url, "[%?&]response_format=json") then
        j = json_get(json, "folder_content")
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
          io.stdout:write("Sort unsupported.\n")
          io.stdout:flush()
          abortgrab = true
          return urls
        end
        for _, d in pairs(json_get(j, sort)) do
          discovered["id:" .. json_get(d, keyname)] = true
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
  local items = nil
  for item, _ in pairs(discovered) do
    print('found item', item)
    if items == nil then
--      items = item
    else
--      items = items .. "\0" .. item
    end
  end

  if items ~= nil then
    local tries = 0
    while tries < 10 do
      local body, code, headers, status = http.request(
        "http://blackbird-amqp.meo.ws:23038/mediafire-db23vdgfp6gfwzx/",
        items
      )
      if code == 200 or code == 409 then
        break
      end
      io.stdout:write("Could not queue discovered items. Sleeping...\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == 10 then
      io.stdout:write("Too many tries.\n")
      io.stdout:flush()
      abortgrab = true
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end

