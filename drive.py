import time
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from threading import Thread

MIME_TYPE = {
    "folder": "application/vnd.google-apps.folder",
    "file": "application/vnd.google-apps.file",
}


class AccessDrive:
    def __init__(self):
        """
        A class for accessing google drive via a service account
        """
        gauth = GoogleAuth()
        gauth.ServiceAuth()
        self.drive = GoogleDrive(gauth)
        self.thread = Thread()
        self.thread.start()
        self.visit_all()

    def visit_all(self, root={"id": "root"}):
        """
        A helper method that visits all possible files

        This is useful because visiting all files before hand speeds up the process of finding and
        uploading/downloading from files by 10x
        """
        for file in self.drive.ListFile(
            {"q": f"'{root['id']}' in parents and trashed = false"}
        ).GetList():
            if file["mimeType"] == MIME_TYPE["folder"]:
                self.visit_all(file)

    def tokenize_path(self, path):
        """
        A helper method that tokenizes the file path to make searching for the files easier
        """
        assert "//" not in path
        path = path.strip().lstrip("/").split("/")
        tmp = list(map(lambda s: s + "/", path[:-1]))
        path = tmp if path[-1] == "" else tmp + [path[-1]]
        return path

    def create_file(self, parent_id, name):
        """
        Returns a file with a given parent and name, and does not create duplicates
        """
        assert isinstance(name, str)
        meta = {"title": f"{name.strip("/")}", "parents": [{"id": parent_id}]}
        if name[-1] == "/":
            meta["mimeType"] = MIME_TYPE["folder"]
        f = self.file_query(name, parent_id)
        if f != None:
            return f
        else:
            f = self.drive.CreateFile(meta)
            f.Upload()
            return f

    def file_query(self, name, parent_id):
        """
        A helper method that returns the file with the specifications of some name and parent id

        If the file exists then returns the file, else returns None
        """
        file_name = name.strip(" /")
        d = None
        if name.strip()[-1] == "/":
            d = {
                "q": f"""'{parent_id}' in parents and mimeType = '{MIME_TYPE['folder']}' and trashed = false"""
            }
        else:
            d = {"q": f"""'{parent_id}' in parents and trashed = false"""}
        fl = self.drive.ListFile(d).GetList()
        fl = list(filter(lambda f: f["title"] == file_name, fl))
        if len(fl) != 0:
            return fl[0]
        else:
            return None

    def get_file(self, file_path, create=True):
        """
        Returns a file at a given file path and has the option of creating directories along the
        path if they do not exist

        The default is that directories are created if necessary
        """
        assert file_path.strip(" /") != ""
        # strip the strings of empty characters
        file_path = self.tokenize_path(file_path)

        file_id = "root"
        # if the target file is not in the root then propagate down the path
        for seg in file_path[:-1]:
            # there should only be one such file in existence if not I did something wrong and
            # needs to be fixed
            file = self.file_query(seg, file_id)
            if file == None:
                if not create:
                    return None
                file = self.create_file(file_id, seg)
            file_id = file["id"]
        return self.create_file(file_id, file_path[-1])

    def from_string(self, file_path, content):
        """
        Creates/updates a file at some file path and updates its' contents with a given string
        """
        if file_path.strip()[-1] != "/":
            f = self.get_file(file_path)
            f.SetContentString(content)
            self.thread.join()
            self.thread = Thread(target=f.Upload)
            self.thread.start()

    def from_path(self, file_path, local_path):
        """
        Creates/updates a file at some file path and updates its' contesnts with a local file
        """
        if file_path.strip()[-1] != "/":
            f = self.get_file(file_path)
            f.SetContentFile(local_path)
            self.thread.join()
            self.thread = Thread(target=f.Upload)
            self.thread.start()

    def download_to_local(self, file_path, local_path):
        """
        Downloads the contents of a file at some file path in the drive to a local file
        """
        if file_path.strip()[-1] != "/":
            self.thread.join()
            f = self.get_file(file_path)
            f.GetContentFile(local_path)

    def download_to_var(self, file_path):
        """
        Similar to download_path, but instead of uploading contents to a local file it returns the
        contents
        """
        if file_path.strip()[-1] != "/":
            self.thread.join()
            s = time.time()
            f = self.get_file(file_path)
            print("get file:", time.time()-s)
            v = f.GetContentString()
            print("get_cont:", time.time()-s)
            return v 

    def delete(self, path=None):
        """
        Goes down a path to delete a file

        If the oath does not exist, it raises an error
        If not argument is given, everything in the drive is deleted
        """
        if isinstance(path, str):
            path.strip()
        file_list = self.drive.ListFile(
            {"q": "'root' in parents and trashed=false"}
        ).GetList()
        if path == None:
            for file in file_list:
                Thread(target=file.Trash).start()
        else:
            file = self.get_id(path, create=False)
            if file != None:
                Thread(target=file.Trash).start()

    def toTree(self, root={"id": "root", "title": "root"}):
        """
        A helper method that traverses all possible files and folders and making a tree with
        the titles of the files/folders

        @parameter root: the file/folder and NOT the id, in the case of a folder it will traverse
        its' children else returning the name of the file
        """
        meta = {"q": "'%s' in parents and trashed=false" % root["id"]}
        file_list = self.drive.ListFile(param=meta).GetList()
        children = []
        for file in file_list:
            if file["mimeType"] == MIME_TYPE["folder"]:
                children.append(self.toTree(file))
            else:
                children.append(file["title"])
        return {root["title"] + "/": children}

    def print_helper(self, tree, buff=0):
        """
        Helper method that takes in a tree and prints it out as a human readable string
        """
        prefix = buff * "|"
        if isinstance(tree, dict):
            for k in tree.keys():
                print(prefix + k)
                for b in tree[k]:
                    self.print_helper(b, buff + 1)
        else:
            print(prefix + tree)

    def toString(self):
        """
        Uses the toTree and print_helper methods to print out a human readable tree of all the
        files contained in the drive
        """
        self.print_helper(self.toTree())
