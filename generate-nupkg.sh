#!/bin/bash

. /etc/init.d/functions

# This awesome snippet was taken from John Kugelman response on StackOverflow
# https://stackoverflow.com/a/5196220
step() {
    echo -n "$@"

    STEP_OK=0
    [[ -w /tmp ]] && echo $STEP_OK > /tmp/step.$$
}

try() {
    # Check for `-b' argument to run command in the background.
    local BG=

    [[ $1 == -b ]] && { BG=1; shift; }
    [[ $1 == -- ]] && {       shift; }

    # Run the command.
    if [[ -z $BG ]]; then
        "$@"
    else
        "$@" &
    fi

    # Check if command failed and update $STEP_OK if so.
    local EXIT_CODE=$?

    if [[ $EXIT_CODE -ne 0 ]]; then
        STEP_OK=$EXIT_CODE
        [[ -w /tmp ]] && echo $STEP_OK > /tmp/step.$$

        if [[ -n $LOG_STEPS ]]; then
            local FILE=$(readlink -m "${BASH_SOURCE[1]}")
            local LINE=${BASH_LINENO[0]}

            echo "$FILE: line $LINE: Command \`$*' failed with exit code $EXIT_CODE." >> "$LOG_STEPS"
        fi
    fi

    return $EXIT_CODE
}

next() {
    [[ -f /tmp/step.$$ ]] && { STEP_OK=$(< /tmp/step.$$); rm -f /tmp/step.$$; }
    [[ $STEP_OK -eq 0 ]]  && echo_success || echo_failure
    echo

    return $STEP_OK
}
# End of snippet

step Packaging Figlotech.Core
try dotnet pack Figlotech.Core -o ..\_nupkg
next 

step Packaging Figlotech.Core
try dotnet pack Figlotech.BDados -o ..\_nupkg
next 

step Packaging Figlotech.Core
try dotnet pack Figlotech.BDados.MySqlDataAccessor -o ..\_nupkg
next 

step Packaging Figlotech.Core
try dotnet pack Figlotech.BDados.SQLiteDataAccessor -o ..\_nupkg
next 

step Packaging Figlotech.Core
try dotnet pack Figlotech.ExcelUtil -o ..\_nupkg
next 

pause >nul