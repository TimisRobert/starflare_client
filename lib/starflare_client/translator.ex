defmodule StarflareClient.Translator do
  @moduledoc false

  @doc false
  # OTP21 and after
  def translate(min_level, :error, :report, {:logger, %{label: label} = report}) do
    case label do
      {:gen_statem, :terminate} ->
        do_translate(min_level, report)

      _ ->
        :none
    end
  end

  def translate(_min_level, _level, _kind, _message) do
    :none
  end

  # OTP21 and after
  defp do_translate(min_level, report) do
    inspect_opts = Application.get_env(:logger, :translator_inspect_opts)

    %{name: name, state: state} = report

    msg = ["GenStateMachine #{inspect(name)} terminating", statem_exception(report)]

    if min_level == :debug do
      msg = [msg, "\nState: ", inspect(state, inspect_opts)]
      {:ok, statem_debug(report, inspect_opts, msg)}
    else
      {:ok, msg}
    end
  end

  # OTP21 and later
  defp statem_exception(%{reason: {class, reason, stack}}) do
    do_statem_exception(class, reason, stack)
  end

  defp do_statem_exception(class, reason, stack) do
    formatted = Exception.format(class, reason, stack)
    [?\n | :erlang.binary_part(formatted, 0, byte_size(formatted) - 1)]
  end

  defp statem_debug(args, opts, msg) do
    [msg, Enum.map(Enum.sort(args), &translate_arg(&1, opts))]
  end

  defp translate_arg({:last_event, last_event}, opts),
    do: ["\nLast event: " | [inspect(last_event, opts)]]

  defp translate_arg({:state, state}, opts),
    do: ["\nState: " | [inspect(state, opts)]]

  defp translate_arg({:callback_mode, callback_mode}, opts),
    do: ["\nCallback mode: " | [inspect(callback_mode, opts)]]

  defp translate_arg({:queued, queued}, opts),
    do: ["\nQueued events: " | [inspect(queued, opts)]]

  defp translate_arg({:queue, [last | queued]}, opts),
    do: ["\nLast event: ", inspect(last, opts), "\nQueued events: " | [inspect(queued, opts)]]

  defp translate_arg({:postponed, postponed}, opts),
    do: ["\nPostponed events: " | [inspect(postponed, opts)]]

  defp translate_arg(_arg, _opts), do: []
end
