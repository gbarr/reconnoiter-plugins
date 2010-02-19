-- Copyright (c) 2010, Graham Barr
--
-- Permission is hereby granted, free of charge, to any person
-- obtaining a copy of this software and associated documentation
-- files (the "Software"), to deal in the Software without
-- restriction, including without limitation the rights to use,
-- copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the
-- Software is furnished to do so, subject to the following
-- conditions:
--
-- The above copyright notice and this permission notice shall be
-- included in all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
-- EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
-- OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
-- NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
-- HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
-- WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
-- FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
-- OTHER DEALINGS IN THE SOFTWARE.

-- This connects to a beanstalkd instance
-- It issues stats commands and translates the output into metrics

module(..., package.seeall)

function onload(image)
  image.xml_description([=[
<module>
  <name>beanstalkd</name>
  <description><para>Monitor management metrics of a beabstalkd instance.</para></description>
  <loader>lua</loader>
  <object>noit.module.beanstalkd</object>
  <moduleconfig />
  <checkconfig>
    <parameter name="port" required="optional" default="11300"
               allowed="\d+">Specifies the port on which beanstalkd can be reached.</parameter>
    <parameter name="tubes" required="optional" default=".*"
               allowed=".+">Specifies a regular expression to pick which tubes to reports metrics for.</parameter>
  </checkconfig>
  <examples>
    <example>
      <title>Monitor two beanstalk instances</title>
      <para>The following example pulls all metrics available from beanstalkd running on 10.1.2.3 and 10.1.2.4</para>
      <programlisting><![CDATA[
      <noit>
        <modules>
          <loader image="lua" name="lua">
            <config><directory>/opt/reconnoiter/libexec/modules-lua/?.lua</directory></config>
          </loader>
          <module loader="lua" name="beanstalkd" object="noit.module.beanstalkd"/>
        </modules>
        <checks>
          <check uuid="2d42adbc-7c7a-11dd-a48f-4f59e0b654d3" module="beanstalkd" target="10.1.2.3" />
          <check uuid="324c2234-7c7a-11dd-8585-cbb783f8267f" module="beanstalkd" target="10.1.2.4" />
        </checks>
      </noit>
      ]]></programlisting>
    </example>
  </examples>
</module>
]=]);
  return 0
end

function init(module)
  return 0
end

function config(module, options)
  return 0
end

function get_tubes(e)
  local tubes = {}
  local str

  e:write("list-tubes\r\n");
  str = e:read("\n")
  if str == nil or string.find(str,"^OK") == nil then
    error(str or "Unexpected EOF")
  end

  while true do
    str = e:read("\n");
    if str == nil then error("Unexpected EOF") end
    str = string.gsub(str,"[\r\n]+","")
    if str == "" then break end
    local t = string.match(str,"^- ([^%c%s]+)")
    if t ~= nil then
      tubes[t] = 1
    elseif str ~= "---" then
      error("Unexpected '" .. str .. "'")
    end
  end
  return tubes
end

function get_stats(e,cmd)
  local stats = {}
  local str

  e:write(cmd);
  str = e:read("\n")
  if str == nil or string.find(str,"^OK") == nil then
    if string.find(str,"^NOT_FOUND") then
      return stats
    else
      error(str or "Unexpected EOF")
    end
  end

  while true do
    str = e:read("\n");
    if str == nil then error("Unexpected EOF") end
    str = string.gsub(str,"[\r\n]+","")
    if str == "" then break end
    local k, v = string.match(str,"^([^%c%s]+):%s+([^%s]+)")
    if v ~= nil then
      stats[k] = v
    elseif str ~= "---" then
      error("Unexpected '" .. str .. "'")
    end
  end
  return stats
end

function n(s)
  local n = tonumber(s)
  if n == nil then
    error("Not a number '" .. s .. "'")
  end
  return n
end

function initiate(module, check)
  local e = noit.socket()

  -- expect the worst
  check.bad()
  check.unavailable()

  local rv, err = e:connect(check.target, check.config.port or 11300)

  if rv ~= 0 then error(err or "unknown error") end

  local results = { }

  local stats = get_stats(e, "stats\r\n")
  local total_jobs = 0

  results["current-jobs-urgent"]   = n(stats["current-jobs-urgent"])
  results["current-jobs-ready"]    = n(stats["current-jobs-ready"])
  results["current-jobs-reserved"] = n(stats["current-jobs-reserved"])
  results["current-jobs-delayed"]  = n(stats["current-jobs-delayed"])
  results["current-jobs-buried"]   = n(stats["current-jobs-buried"])
  results["job-timeouts"]          = n(stats["job-timeouts"])
  results["total-connections"]     = n(stats["total-connections"])
  results["total-jobs"]            = n(stats["total-jobs"])
  results["total-commands"]        = 0

  results["current-jobs-todo"]     = n(stats["current-jobs-urgent"])
                                   + n(stats["current-jobs-ready"])
                                   + n(stats["current-jobs-reserved"])
                                   + n(stats["current-jobs-delayed"])
                                   + n(stats["current-jobs-buried"])
  results["total-done"]            = n(stats["total-jobs"]) - results["current-jobs-todo"]

  if n(stats["binlog-current-index"]) > 0 then
    results["binlog-size"] = (1 + n(stats["binlog-current-index"]) - n(stats["binlog-oldest-index"])) * n(stats["binlog-max-size"])
  else
    results["binlog-size"] = 0
  end

  for k, v in pairs(stats) do
    if string.find(k,"^cmd-") then
      results["total-commands"] = results["total-commands"] + n(v)
    end
  end

  local tubes = get_tubes(e)
  local tubere = noit.pcre(check.config.tubes or ".*")

  for tube, i in pairs(tubes) do
    if tubere ~= nil and tubere(tube) then
      local tube_stats = get_stats(e, "stats-tube " .. tube .. "\r\n")
    
      results[tube .. '`' .. "current-jobs-urgent"]   = n(tube_stats["current-jobs-urgent"])
      results[tube .. '`' .. "current-jobs-ready"]    = n(tube_stats["current-jobs-ready"])
      results[tube .. '`' .. "current-jobs-reserved"] = n(tube_stats["current-jobs-reserved"])
      results[tube .. '`' .. "current-jobs-delayed"]  = n(tube_stats["current-jobs-delayed"])
      results[tube .. '`' .. "current-jobs-buried"]   = n(tube_stats["current-jobs-buried"])
      results[tube .. '`' .. "total-jobs"]            = n(tube_stats["total-jobs"])
      results[tube .. '`' .. "current-watching"]      = n(tube_stats["current-watching"])

      results[tube .. '`' .. "current-jobs-todo"]     = n(tube_stats["current-jobs-urgent"])
                                                      + n(tube_stats["current-jobs-ready"])
                                                      + n(tube_stats["current-jobs-reserved"])
                                                      + n(tube_stats["current-jobs-delayed"])
                                                      + n(tube_stats["current-jobs-buried"])
      results[tube .. '`' .. "total-done"]            = n(tube_stats["total-jobs"]) - results[tube .. '`' .. "current-jobs-todo"]
    end
  end

  local i = 0

  for k, v in pairs(results) do
    check.metric_uint64(k,v)
    i = i + 1
  end

  if i > 0 then check.available() end
  check.status(string.format("%d stats", i))
  check.good()
end

