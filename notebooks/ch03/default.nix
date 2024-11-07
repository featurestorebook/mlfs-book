with import <nixpkgs> { };

let
  pythonPackages = python312Packages; # Change to Python 3.12
in pkgs.mkShell rec {
  name = "impurePythonEnv";
  venvDir = "./.venv";
  buildInputs = [

    pkgs.stdenv.cc.cc.lib

    git-crypt
    stdenv.cc.cc # jupyter lab needs

    # pythonPackages.python
    pythonPackages.ipykernel
    pythonPackages.jupyterlab
    pythonPackages.pyzmq    # Adding pyzmq explicitly
    pythonPackages.venvShellHook
    pythonPackages.pip
    pythonPackages.wheel
    pythonPackages.thrift
    pythonPackages.pandas


    # sometimes you might need something additional like the following - you will get some useful error if it is looking for a binary in the environment.
    taglib
    openssl
    git
    libxml2
    libxslt
    libzip
    zlib

  ];

  # Run this command, only after creating the virtual environment
  postVenvCreation = ''
    unset SOURCE_DATE_EPOCH
    
    python -m ipykernel install --user --name=myenv4 --display-name="myenv4"
    pip install -r requirements.txt
  '';

  # Now we can execute any commands within the virtual environment.
  # This is optional and can be left out to run pip manually.
  postShellHook = ''
    # allow pip to install wheels
    unset SOURCE_DATE_EPOCH
  '';

  # # Ensure that Jupyter can find the kernel
  # shellHook = ''
  #   export JUPYTER_PATH=${pkgs.python312Packages.jupyterlab}/share/jupyter
      # export PYTHONPATH=$PWD/$venvDir/${python.sitePackages}:$PYTHONPATH
  # '';
}