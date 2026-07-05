ExUnit.start()

if System.get_env("REPLICANT_TEST_URL") in [nil, ""] do
  ExUnit.configure(exclude: [:integration])
end
