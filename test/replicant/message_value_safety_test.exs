defmodule Replicant.MessageValueSafetyTest do
  @moduledoc """
  R03 adversarial value-safety tripwires (Critical Rule 1).

  A logical-decoding message's `prefix` and `content` are **user-controlled bytes** — a
  `pg_logical_emit_message(_, prefix, content)` caller chooses them, so they can carry a
  secret, a row value, or an attacker-influenced payload. This module drives a hostile
  prefix/content through EVERY replicant-owned failure surface and asserts the bytes never
  appear in the resulting `%Replicant.Error{}`, in a raise/throw/exit that escapes the
  boundary, or in a telemetry event. Replicant is prefix-blind: it never routes on the
  prefix, so an unknown/hostile prefix yields only the structural reason atom.

  Non-vacuity: each surface's guard is RED-capable by mutating the exact scrub it protects
  (documented per-test). The mutations were exercised RED-before-green during the R03 build:

    * decode scrub — `Error.decode_failure/1` leaking `Exception.message/1` into `:shape`,
      exercised separately before prefix framing and after content framing.
    * sink-fault scrub — both `Assembler.deliver_message/2` and transaction delivery leaking
      caught exceptions, throws/exits, or bad return reasons.
    * before-Begin halt — interpolating `prefix` into the static `shape:` string.
    * telemetry — asserted the handler actually fired, so an absent-bytes assertion is not
      vacuously true over an event that never arrived.

  Unit-level (no live PostgreSQL): the real `Decoder`, `Assembler`, `Error`, and `Telemetry`
  code runs against synthetic hostile bytes. The live-PG delivery marquees are unchanged in
  `test/integration/messages_test.exs`.
  """
  use ExUnit.Case, async: false

  alias Replicant.{Assembler, Decoder, Error}
  alias Replicant.Decoder.Messages.Message

  # Distinctive sentinels that must never survive any failure surface. Both carry a NUL and an
  # invalid-UTF-8 byte (\xff) so a leak that renders as a raw bitstring (not a printable string)
  # is exercised too — that is the realistic shape of a leaked row/secret byte.
  @hostile_prefix "HOSTILE_PREFIX_\x00\xffcafebabe"
  @wire_prefix "HOSTILE_WIRE_PREFIX_cafebabe"
  @secret_content "SECRET_CONTENT_deadbeef_\x00row-bytes"

  # A leaked sentinel can surface TWO ways depending on the leak vector: (a) as a printable
  # substring when the leaked value is a valid-UTF-8 binary that `inspect` quotes, or (b) as a
  # comma-separated DECIMAL byte-list when the leaked value is a non-UTF-8 binary that `inspect`
  # renders as `<<72, 79, 83, ...>>`. A substring-only check misses (b) — the exact byte-level
  # evasion this project has been bitten by before. Exception formatters may insert line breaks
  # before the term reaches this helper, so the byte signature permits arbitrary whitespace.
  defp refute_leak(rendered, sentinel, label) do
    ascii = sentinel |> :binary.bin_to_list() |> Enum.filter(&(&1 in 32..126)) |> List.to_string()
    refute rendered =~ ascii, "#{label} leaked (printable form) into: #{rendered}"

    byte_pattern =
      sentinel
      |> :binary.bin_to_list()
      |> Enum.map_join("\\s*,\\s*", &Integer.to_string/1)
      |> Regex.compile!()

    refute Regex.match?(byte_pattern, rendered),
           "#{label} leaked (byte-list form) into: #{rendered}"

    rendered
  end

  # Assert neither sentinel appears anywhere in a rendered term (default inspect + Error.message).
  defp assert_value_free(term) do
    rendered = inspect(term, limit: :infinity, printable_limit: :infinity, width: :infinity)
    refute_leak(rendered, @hostile_prefix, "prefix")
    refute_leak(rendered, @secret_content, "content")
    term
  end

  defp assert_error_message_free(%Error{} = err) do
    msg = Exception.message(err)
    refute_leak(msg, @hostile_prefix, "prefix (Error.message/1)")
    refute_leak(msg, @secret_content, "content (Error.message/1)")
    err
  end

  defp assert_wire_prefix_free(%Error{} = err) do
    rendered = inspect(err, limit: :infinity, printable_limit: :infinity, width: :infinity)
    refute_leak(rendered, @wire_prefix, "wire prefix")
    refute_leak(Exception.message(err), @wire_prefix, "wire prefix (Error.message/1)")
    err
  end

  # Build a raw pgoutput 'M' frame: flags::8, lsn::64, prefix NUL-terminated, length::32, content.
  defp message_frame(flags, prefix, content, declared_length) do
    <<"M", flags::8, 0::64>> <>
      prefix <> <<0>> <> <<declared_length::32>> <> content
  end

  describe "decode boundary (Decoder.decode/1)" do
    test "a malformed 'M' frame without a prefix terminator cannot leak prefix bytes" do
      # No NUL appears after the fixed header, so the two-element split match fails while the
      # MatchError still contains the complete attacker-controlled prefix. This specifically
      # protects the prefix side of the decode boundary; a frame that reaches the length match
      # has already split the prefix out of the failing term.
      # MUTATION that proves this RED: `Error.decode_failure/1` set `shape: Exception.message(exception)`.
      frame = <<"M", 0, 0::64>> <> @wire_prefix <> <<0xFF>>

      assert {:error, %Error{reason: :decode_failure} = err} = Decoder.decode(frame)
      assert_wire_prefix_free(err)
      assert err.shape == "MatchError"
    end

    test "a malformed 'M' frame with truncated content cannot leak content bytes" do
      # declared_length > actual content → the length/content match raises after prefix framing;
      # its MatchError contains the raw content bytes. decode/1 must retain only the exception
      # module name, never the failed match term.
      # MUTATION that proves this RED: `Error.decode_failure/1` set `shape: Exception.message(exception)`.
      frame = message_frame(0, @wire_prefix, @secret_content, 9_999)

      assert {:error, %Error{reason: :decode_failure} = err} = Decoder.decode(frame)
      assert_value_free(err)
      assert_error_message_free(err)
      assert err.shape == "MatchError"
    end

    test "a well-formed hostile-prefix 'M' frame decodes to a struct WITHOUT surfacing bytes in any error" do
      # A valid non-transactional message carries the hostile bytes on the %Message{} struct —
      # that is correct (they are handed to the sink). The value-safety contract is about FAILURE
      # surfaces, so here we assert decode succeeds and produces no error term at all.
      #
      # The wire prefix is a NUL-terminated C-string, so it cannot itself embed a NUL; use a
      # NUL-free (but still hostile/greppable) prefix here. The content IS length-prefixed and may
      # carry arbitrary bytes including NUL (@secret_content does).
      frame_prefix = "HOSTILE_PREFIX_cafebabe"
      frame = message_frame(0, frame_prefix, @secret_content, byte_size(@secret_content))

      assert {:ok,
              %Message{transactional?: false, prefix: ^frame_prefix, content: @secret_content}} =
               Decoder.decode(frame)
    end
  end

  describe "sink-callback failure surfaces (Assembler.handle_message/2 → deliver_message/2)" do
    setup do
      msg = %Message{
        transactional?: false,
        lsn: 0x16E_3778,
        prefix: @hostile_prefix,
        content: @secret_content
      }

      %{msg: msg}
    end

    defmodule RaiseSink do
      @moduledoc false
      @behaviour Replicant.Sink
      def handle_transaction(_txn), do: {:ok, 0}
      # The exception message embeds the user bytes — the classic Rule-1 leak vector.
      def handle_message(%{prefix: p, content: c}, _ctx),
        do: raise("sink boom prefix=#{p} content=#{c}")
    end

    defmodule ThrowSink do
      @moduledoc false
      @behaviour Replicant.Sink
      def handle_transaction(_txn), do: {:ok, 0}
      def handle_message(%{prefix: p, content: c}, _ctx), do: throw({:evil, p, c})
    end

    defmodule ExitSink do
      @moduledoc false
      @behaviour Replicant.Sink
      def handle_transaction(_txn), do: {:ok, 0}
      def handle_message(%{prefix: p, content: c}, _ctx), do: exit({:evil_exit, p, c})
    end

    defmodule BadReturnSink do
      @moduledoc false
      @behaviour Replicant.Sink
      def handle_transaction(_txn), do: {:ok, 0}
      # A non-:ok return term carrying the user bytes; the scrub must not echo it.
      def handle_message(%{prefix: p, content: c}, _ctx), do: {:error, {:leak, p, c}}
    end

    # MUTATION that proves all four RED: `deliver_message/2`'s `_other` clause set
    # `shape: inspect(reason)` (echoing the caught reason / bad return).
    for {label, sink} <- [
          {"a sink RAISE carrying user bytes", RaiseSink},
          {"a sink THROW carrying user bytes", ThrowSink},
          {"a sink EXIT carrying user bytes", ExitSink},
          {"a sink non-:ok RETURN carrying user bytes", BadReturnSink}
        ] do
      test "#{label} halts value-free with :sink_failed", %{msg: msg} do
        asm = Assembler.new(unquote(sink))

        assert {:halt, %Error{reason: :sink_failed} = err, _asm} =
                 Assembler.handle_message(asm, msg)

        assert_value_free(err)
        assert_error_message_free(err)
      end
    end
  end

  describe "transactional-message sink failure surfaces (Commit → handle_transaction/1)" do
    defmodule TransactionRaiseSink do
      @moduledoc false
      @behaviour Replicant.Sink

      def handle_transaction(%{messages: [%{prefix: p, content: c}]}),
        do: raise("transaction sink boom prefix=#{p} content=#{c}")
    end

    defmodule TransactionThrowSink do
      @moduledoc false
      @behaviour Replicant.Sink

      def handle_transaction(%{messages: [%{prefix: p, content: c}]}),
        do: throw({:transaction_evil, p, c})
    end

    defmodule TransactionExitSink do
      @moduledoc false
      @behaviour Replicant.Sink

      def handle_transaction(%{messages: [%{prefix: p, content: c}]}),
        do: exit({:transaction_evil_exit, p, c})
    end

    defmodule TransactionBadReturnSink do
      @moduledoc false
      @behaviour Replicant.Sink

      def handle_transaction(%{messages: [%{prefix: p, content: c}]}),
        do: {:error, {:transaction_leak, p, c}}
    end

    # RED mutations exercised independently at the transaction boundary:
    #   * `{:sink_raised, e}` passed `Exception.message(e)` as the error shape;
    #   * `{:sink_caught, _kind, reason}` passed `inspect(reason)` as the error shape;
    #   * `{:error, reason}` passed `inspect(reason)` as the error shape.
    for {label, sink} <- [
          {"a transaction sink RAISE carrying message bytes", TransactionRaiseSink},
          {"a transaction sink THROW carrying message bytes", TransactionThrowSink},
          {"a transaction sink EXIT carrying message bytes", TransactionExitSink},
          {"a transaction sink non-:ok RETURN carrying message bytes", TransactionBadReturnSink}
        ] do
      test "#{label} halts value-free with :sink_failed" do
        asm = Assembler.new(unquote(sink))

        {:ok, asm} =
          Assembler.handle_message(asm, %Replicant.Decoder.Messages.Begin{
            final_lsn: 100,
            xid: 1
          })

        {:ok, asm} =
          Assembler.handle_message(asm, %Message{
            transactional?: true,
            lsn: 100,
            prefix: @hostile_prefix,
            content: @secret_content
          })

        assert {:halt, %Error{reason: :sink_failed} = err, _asm} =
                 Assembler.handle_message(asm, %Replicant.Decoder.Messages.Commit{lsn: 100})

        assert_value_free(err)
        assert_error_message_free(err)
      end
    end
  end

  describe "structural halt surfaces" do
    test "a transactional message before any Begin halts with ONLY the structural reason (unknown-prefix blind)" do
      # txn: nil + transactional?: true + xid: nil → the config_invalid halt. Its `shape:` is a
      # STATIC description, never the prefix. MUTATION that proves RED: interpolate `prefix` into
      # the `shape:` string.
      asm = Assembler.new(BeforeBeginSink)

      msg = %Message{
        transactional?: true,
        xid: nil,
        lsn: 1,
        prefix: @hostile_prefix,
        content: @secret_content
      }

      assert {:halt, %Error{reason: :config_invalid} = err, _asm} =
               Assembler.handle_message(asm, msg)

      # The structural shape is present; the hostile prefix is not.
      assert err.shape == "transactional message before Begin"
      assert_value_free(err)
      assert_error_message_free(err)
    end
  end

  defmodule BeforeBeginSink do
    @moduledoc false
    @behaviour Replicant.Sink
    def handle_transaction(_txn), do: {:ok, 0}
    def handle_message(_msg, _ctx), do: :ok
  end

  describe "telemetry surfaces carry no message bytes" do
    defmodule OkSink do
      @moduledoc false
      @behaviour Replicant.Sink
      def handle_transaction(_txn), do: {:ok, 0}
      def handle_message(_msg, _ctx), do: :ok
    end

    defmodule FailSink do
      @moduledoc false
      @behaviour Replicant.Sink
      def handle_transaction(_txn), do: {:ok, 0}
      def handle_message(%{prefix: p, content: c}, _ctx), do: {:error, {p, c}}
    end

    setup do
      test_pid = self()
      ref = make_ref()

      handler_id = {__MODULE__, ref}

      :telemetry.attach_many(
        handler_id,
        [
          [:replicant, :message, :received],
          [:replicant, :sink, :failed]
        ],
        fn event, measurements, metadata, _cfg ->
          send(test_pid, {:telemetry, ref, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)
      %{ref: ref}
    end

    test "the [:message, :received] event on a delivered hostile message carries only byte_size + lsn + flag",
         %{ref: ref} do
      asm = Assembler.new(OkSink)

      msg = %Message{
        transactional?: false,
        lsn: 0x16E_3778,
        prefix: @hostile_prefix,
        content: @secret_content
      }

      assert {:message_delivered, _lsn, _asm} = Assembler.handle_message(asm, msg)

      # Non-vacuity: the event MUST have fired (else the absence assertions are hollow).
      assert_receive {:telemetry, ^ref, [:replicant, :message, :received], measurements,
                      metadata},
                     500

      assert metadata == %{
               commit_lsn: 0x16E_3778,
               byte_size: byte_size(@secret_content),
               transactional: false
             }

      assert_value_free(measurements)
      assert_value_free(metadata)
    end

    test "the [:sink, :failed] event on a hostile-message fault carries only the structural reason",
         %{ref: ref} do
      asm = Assembler.new(FailSink)

      msg = %Message{
        transactional?: false,
        lsn: 0x16E_3778,
        prefix: @hostile_prefix,
        content: @secret_content
      }

      assert {:halt, %Error{reason: :sink_failed}, _asm} = Assembler.handle_message(asm, msg)

      assert_receive {:telemetry, ^ref, [:replicant, :sink, :failed], measurements, metadata}, 500

      assert metadata == %{reason: :sink_failed}
      assert_value_free(measurements)
      assert_value_free(metadata)
    end
  end
end
