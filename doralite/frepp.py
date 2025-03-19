import doralite as dl
import glob
import os
import warnings
import copy


def is_consecutive(lst, start=None, end=None, step=1):
    """
    Checks if the list is consecutive with a given step (default=1),
    considering missing values at the beginning and end of the series.
    """
    if not lst:
        return False  # An empty list is not consecutive

    lst = sorted(lst)  # Ensure list is sorted

    # Extrapolate backwards from the first element if start is given
    if start is not None:
        first = lst[0]
        while first - step >= start:
            first -= step
        if first != start:
            return False  # If extrapolated start is not equal to given start, it's not consecutive

    # Extrapolate forwards from the last element if end is given
    if end is not None:
        last = lst[-1]
        while last + step <= end:
            last += step
        if last != end:
            return False  # If extrapolated end is not equal to given end, it's not consecutive

    return all(lst[i] == lst[i - 1] + step for i in range(1, len(lst)))


def find_gaps(lst, start=None, end=None, step=1):
    """
    Identifies missing values in the sequence based on a given step size,
    including gaps at the beginning and end of the expected range.
    """
    if not lst:
        return []  # No gaps in an empty list

    lst = sorted(lst)  # Ensure list is sorted
    gaps = []

    # Extrapolate backwards to determine the true starting point
    if start is not None:
        first = lst[0]
        while first - step >= start:
            first -= step
            gaps.append(first)  # Capture missing values at the beginning

    # Extrapolate forwards to determine the true ending point
    if end is not None:
        last = lst[-1]
        while last + step <= end:
            last += step
            gaps.append(last)  # Capture missing values at the end

    # Find missing values within the list range
    expected = lst[0] + step
    for num in lst[1:]:
        while expected < num:
            gaps.append(expected)
            expected += step
        expected = num + step  # Move to the next expected value

    return sorted(gaps)  # Ensure gaps are returned in order


class history:
    def __init__(self, path, start=None, end=None):
        self.directory = path.replace("pp", "history")
        assert os.path.exists(
            self.directory
        ), f"History dir does not exist: {self.directory}"
        self.files = [
            os.path.basename(x) for x in sorted(glob.glob(f"{self.directory}/*.tar"))
        ]
        self.years = [int(x[0:4]) for x in self.files]

    def consecutive(self, start=None, end=None):
        return is_consecutive(self.years, start=None, end=None)

    def gaps(self, start=None, end=None):
        lst = self.years
        return find_gaps(self.years, start=None, end=None)

    def __str__(self):
        return str(self.directory)

    def __repr__(self):
        return f"history: {self.directory}"


class freppfile:
    def __init__(self, path):
        self.path = path
        self.freq = str("/").join(path.split("/")[-3:-1])
        self.filename = os.path.basename(path)
        self.component = self.filename.split(".")[0]
        self.timeperiod = self.filename.split(".")[1]
        self.variable = self.filename.split(".")[2]
        self.startyear = self.timeperiod.split("-")[0][0:4]
        self.endyear = self.timeperiod.split("-")[1][0:4]

    def __str__(self):
        return str(self.path)

    def __repr__(self):
        return f"freppfile: {self.path}"


def check_freq(tsgroup, freq, start=None, end=None):
    grp = copy.copy(tsgroup)
    grp.files = [x for x in grp.files if freq in x.path]
    endyears = sorted(list(set([int(x.endyear) for x in grp.files])))
    chunklen = int(freq.split("/")[1].replace("yr", ""))
    return find_gaps(endyears, start=start, end=end, step=chunklen)


class tsgroup:
    def __init__(self, path, component, start=None, end=None):
        # Get post-processing path from Dora. Save the requested id as it
        # could differ from Dora's master id. Save the full experiment
        # metadata dictionary

        if isinstance(path, dict):
            metadata = path
            path = metadata["pathPP"]
        else:
            metadata = dl.dora_metadata(path)

        metadata["requested_id"] = None if metadata["id"] is None else path
        self.metadata = metadata
        self.id = self.metadata["requested_id"]

        # Check that the experiment and requested component exist
        assert os.path.exists(metadata["pathPP"]), f"Cannot access {metadata['pathPP']}"
        self.path = os.path.abspath(f"{metadata['pathPP']}/{component}")
        assert os.path.exists(self.path), f"Cannot access {component} component"
        self.component = component

        # Get a list of ts files for the component
        self.files = sorted(glob.glob(self.path + "/ts/**/*.nc", recursive=True))

        # Convert filenames to freppfile objects
        self.files = [freppfile(x) for x in self.files]

        # Add information about the history directory
        self.history = history(self.metadata["pathPP"], start=start, end=end)

        # Infer experiment start and end years from the history files
        self.start = self.history.years[0] if start is None else int(start)
        self.end = self.history.years[-1] if end is None else int(end)

        if not self.history.consecutive(start=start, end=end):
            warnings.warn(
                f"History directory is incomplete. Missing years: {self.history.gaps()}"
            )

    @property
    def variables(self):
        return sorted(list(set([x.variable for x in self.files])))

    @property
    def freqs(self):
        return sorted(list(set([x.freq for x in self.files])))

    @property
    def missing(self):
        results = {}
        for freq in self.freqs:
            results[freq] = check_freq(self, freq, start=self.start, end=self.end)
        missing_years = []
        for k, v in results.items():
            missing_years += v
        missing_years = sorted(list(set(missing_years)))
        return missing_years

    def repair(self):
        commands = []

        assert (
            self.id is not None
        ), "Experiment must be registered in dora to do a repair."
        statedir = os.path.abspath(self.metadata["pathDB"].replace("db", "state"))
        statedir = f"{statedir}/postProcess"
        assert os.path.exists(statedir), f"Cannot access state directory: {statedir}"

        # Remove state files if they exist
        statefiles = [f"{statedir}/{self.component}.{year}" for year in self.missing]
        statefiles = [x for x in statefiles if os.path.exists(x)]
        cmd = f"rm -f {str(' ').join(statefiles)}"
        commands.append(cmd)

        # Construct frepp command
        xmlpath = self.metadata["pathXML"]
        xmlpath = xmlpath[:-1] if xmlpath[-1] == "/" else xmlpath
        assert os.path.exists(xmlpath), f"Cannot find xml: {xmlpath}"

        # Infer platform and target
        platformtarget = statedir.split("/")[-3]
        platformtarget = platformtarget.split("-")
        platform = str("-").join(platformtarget[:2])
        target = str(",").join(platformtarget[2:])

        for year in self.missing:
            cmd = f"frepp -s -x {xmlpath} -t {year} -P {platform} -T {target} -d {self.history.directory} -c {self.component} {self.metadata['expName']}"
            commands.append(cmd)

        return commands

    def __str__(self):
        return str(self.path)

    def __repr__(self):
        return f"TimeSeries group {self.path}"


def repair_all_components(id, components=None):
    metadata = dl.dora_metadata(id)
    metadata["requested_id"] = None if metadata["id"] is None else id
    path = metadata["pathPP"]

    if components is not None:
        components = [components] if isinstance(components, str) else components
        assert isinstance(components, list)
    else:
        components = [
            d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))
        ]
        components = sorted([x for x in components if x[0] != "."])

    components = [tsgroup(metadata, x) for x in components]
    commands = [x.repair() for x in components]
    commands = [x for sublist in commands for x in sublist]

    def custom_sort_key(word):
        order = {"r": 0, "f": 1}  # Define custom priority
        first_char = word[0].lower()  # Get first character in lowercase
        return order.get(first_char, 2), word  # Use 2 as default for others

    commands = sorted(commands, key=custom_sort_key)

    return commands
