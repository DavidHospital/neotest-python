local nio = require("nio")
local lib = require("neotest.lib")
local pytest = require("neotest-python.pytest")
local base = require("neotest-python.base")
local Job = require("plenary.job")
local Path = require("plenary.path")

--- Run `pytest --collect-only` with JSON report to respect pytest.ini
local function collect_with_pytest(path, root, python_command)
  -- Ensure pytest-json-report plugin is installed
  local report_file = root .. "/.neotest-report.json"
  local args = vim.tbl_flatten({ python_command, {
    "-m", "pytest",
    "--collect-only", "-q",
    "--disable-warnings",
    "--json-report",
    "--json-report-file=", report_file,
  }})
  -- Execute pytest collection
  Job:new({
    command = args[1],
    args = vim.list_slice(args, 2),
    cwd = root,
  }):sync()

  -- Read report
  if not Path:new(report_file):exists() then
    error("pytest JSON report not found: " .. report_file)
  end
  local report = vim.json.decode(Path:new(report_file):read())

  -- Traverse report to extract test nodes
  local positions = {}
  local function recurse(node)
    if node.nodeid and node.nodeid:match("::") then
      local parts = vim.split(node.nodeid, "::")
      local file = Path:new(root, parts[1]):absolute()
      local name = parts[#parts]
      table.insert(positions, {
        path = file,
        name = name,
        position = { line = 1 },
        scope = "test",
      })
    end
    if node.children then
      for _, child in ipairs(node.children) do
        recurse(child)
      end
    end
  end
  recurse(report.collector)

  -- Clean up
  Path:new(report_file):rm()
  return positions
end

---@param config neotest-python._AdapterConfig
---@return neotest.Adapter
return function(config)
  local function build_script_args(run_args, results_path, stream_path, runner)
    local script_args = {
      "--results-file", results_path,
      "--stream-file", stream_path,
      "--runner", runner,
    }
    if config.pytest_discovery then
      table.insert(script_args, "--emit-parameterized-ids")
    end
    local position = run_args.tree:data()
    table.insert(script_args, "--")
    vim.list_extend(script_args, config.get_args(runner, position, run_args.strategy))
    if run_args.extra_args then
      vim.list_extend(script_args, run_args.extra_args)
    end
    if position then
      table.insert(script_args, position.id)
    end
    return script_args
  end

  return {
    name = "neotest-python",
    root = base.get_root,
    filter_dir = function(name)
      return name ~= "venv"
    end,
    is_test_file = config.is_test_file,
    discover_positions = function(path)
      local root = base.get_root(path) or vim.loop.cwd()
      local python_cmd = config.get_python_command(root)
      local runner = config.get_runner(python_cmd)

      if runner == "pytest" and config.pytest_discovery then
        return collect_with_pytest(path, root, python_cmd)
      end

      -- Fallback: treesitter-based discovery, then pytest augmentation
      local positions = lib.treesitter.parse_positions(
        path,
        base.treesitter_queries(runner, config, python_cmd),
        { require_namespaces = runner == "unittest" }
      )
      if runner == "pytest" then
        pytest.augment_positions(python_cmd, base.get_script_path(), path, positions, root)
      end
      return positions
    end,

    build_spec = function(args)
      local position = args.tree:data()
      local root = base.get_root(position.path) or vim.loop.cwd()
      local python_cmd = config.get_python_command(root)
      local runner = config.get_runner(python_cmd)

      local results_path = nio.fn.tempname()
      local stream_path = nio.fn.tempname()
      lib.files.write(stream_path, "")
      local stream_data, stop_stream = lib.files.stream_lines(stream_path)

      local script_args = build_script_args(args, results_path, stream_path, runner)
      local script_path = base.get_script_path()

      local strategy_config
      if args.strategy == "dap" then
        strategy_config = base.create_dap_config(python_cmd, script_path, script_args, config.dap_args)
      end

      return {
        command = vim.iter({ python_cmd, script_path, script_args }):flatten():totable(),
        context = { results_path = results_path, stop_stream = stop_stream },
        stream = function()
          return function()
            local lines = stream_data()
            local results = {}
            for _, line in ipairs(lines) do
              local result = vim.json.decode(line, { luanil = { object = true } })
              results[result.id] = result.result
            end
            return results
          end
        end,
        strategy = strategy_config,
      }
    end,

    results = function(spec, result)
      spec.context.stop_stream()
      local ok, data = pcall(lib.files.read, spec.context.results_path)
      if not ok then data = "{}" end
      local results = vim.json.decode(data, { luanil = { object = true } })
      for _, r in pairs(results) do
        result.output_path = r.output_path
      end
      return results
    end,
  }
end

