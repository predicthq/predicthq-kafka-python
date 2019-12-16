import rfc3339


def _build_hist_stage_timings(hist, timings=None):
    if timings is None:
        timings = []

    stage_svc = hist['svc']
    stage_end_dt = rfc3339.parse_datetime(hist['dt'])

    timings = [(stage_svc, stage_end_dt)] + timings

    refs = [ref for ref in hist['refs'] if ref.get('hist')]

    if not refs:
        return timings

    previous_hist = sorted(refs, key=lambda r: r['hist']['dt'])[-1]['hist']

    return _build_hist_stage_timings(previous_hist, timings=timings)


# TODO: In which services is this function used and to what end?
def calc_ref_latencies(this_svc, ref, target_svc=None):
    hist = ref['hist']

    if not hist:
        return [], 0

    last_end_dt = rfc3339.now()
    stage_timings = _build_hist_stage_timings(hist)

    if target_svc:
        target_i = None
        for i, (svc, _) in enumerate(stage_timings):
            if svc == target_svc:
                target_i = i
                break

        if target_i is None:
            # We didn't find our target, the message mustn't have
            # flowed through our target, so we can't time from it.
            stage_timings = []
        elif target_i == 0:
            # Target was the first svc, so time from after it.
            # Not quite what we're after - don't have timings from
            # the target itself - but close enough.
            stage_timings = stage_timings
        else:
            # Time from end of service before the target, so we
            # include timings from the target.
            stage_timings = stage_timings[target_i - 1:]

    if not stage_timings:
        return [], 0

    # Add a stage for this service, since it's not in the ref history.
    # Only part way through processing, but will capture any lag at least.
    stage_timings.append((this_svc, last_end_dt))

    first_svc, first_end_dt = stage_timings[0]
    total_latency = (last_end_dt - first_end_dt).total_seconds()

    stage_latencies = []
    # The first service doesn't have a stage, as it's what produced the
    # first message, so we don't have a start time for it.
    # We start timing the second service from the end of the first service.
    current_stage_start_dt = first_end_dt
    current_stage_svc, current_stage_end_dt = stage_timings[1]
    for stage_svc, stage_end_dt in stage_timings[2:]:
        if stage_svc == current_stage_svc:
            current_stage_end_dt = stage_end_dt
        else:
            stage_latency = (current_stage_end_dt - current_stage_start_dt).total_seconds()
            stage_latencies.append((current_stage_svc, stage_latency))

            current_stage_svc = stage_svc
            current_stage_start_dt = current_stage_end_dt
            current_stage_end_dt = stage_end_dt

    stage_latency = (current_stage_end_dt - current_stage_start_dt).total_seconds()
    stage_latencies.append((current_stage_svc, stage_latency))

    return stage_latencies, total_latency
